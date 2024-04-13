from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd

from common.columns import TABLE_META
from common.constants import DEV_MODE, DEV_REDUCED_ROWS, CBD_LANDMARK_ADDRESS, PROXIMITY_RADIUS
from common.utils import calc_dist
from scraper.datagov.resale_price_scraper import ResalePriceScraper
from scraper.onemap.onemap_scraper import OnemapScraper
from reporting.utils import consolidate_report, plot_default_features, plot_mrt_info, create_html_report
from data_preparation.utils import clean_resale_prices_for_visualisation

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='hdb_pipeline', default_args=default_args, schedule=None, catchup=False, tags=['main_dag'], template_searchpath=["/opt/airflow/"])
def hdb_pipeline():

    @task
    def scrape_resale_prices():
        context = get_current_context()
        date = context["execution_date"]
        resale_price_scraper = ResalePriceScraper({}, "live") # use `backfill` for all data and `live` to only scrape latest dataset
        pg_hook = PostgresHook("resale_price_db")
        first_id = None
        for idx, rows in enumerate(resale_price_scraper.run_scrape(date), start=0):
            if DEV_MODE and idx == DEV_REDUCED_ROWS: break
            # necessary to support execute + commit + fetch, pg_hook doesn't support this combination let alone PostgresOperator
            with closing(pg_hook.get_conn()) as conn:
                if pg_hook.supports_autocommit:
                    pg_hook.set_autocommit(conn, True)
                with closing(conn.cursor()) as cursor:
                    cursor.execute(
                        f"""
                        INSERT INTO staging.stg_resale_prices ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id'])})
                        VALUES {",".join(["({})".format(",".join(['%s'] * (len(TABLE_META['stg_resale_prices'].columns)-1)))]*len(rows))}
                        ON CONFLICT ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id' and col != 'remaining_lease'])}) DO NOTHING
                        RETURNING id;
                        """,
                        [val for row in rows for val in row]
                    )
                    curr_id = cursor.fetchone()
                    if curr_id:
                        first_id = first_id if first_id else curr_id
                        first_id = min(first_id, curr_id)
        return first_id[0] if first_id else first_id

    @task
    def enhance_resale_price_coords(min_id: int):
        if not min_id:
            return
        onemap_scraper = OnemapScraper({})
        pg_hook = PostgresHook("resale_price_db")
        with open("/opt/airflow/dags/sql/enhance_resale_price_coords_select.sql", 'r') as file:
            sql_query = file.read()
            new_rows = pg_hook.get_pandas_df(
                sql = sql_query.format(min_id)
            )
        enhanced_rows = onemap_scraper.enhance_resale_price(new_rows)
        # Convert months to datetime format
        enhanced_rows['transaction_month'] = pd.to_datetime(enhanced_rows['transaction_month'])
        # Get location data
        enhanced_rows['latitude'] = pd.to_numeric(enhanced_rows['latitude'], errors='coerce')
        enhanced_rows['longitude'] = pd.to_numeric(enhanced_rows['longitude'], errors='coerce')
        # Drop rows without location data
        enhanced_rows = enhanced_rows[(enhanced_rows['latitude'].notna()) & (enhanced_rows['longitude'].notna())]
        # Persist to data warehouse
        records = [list(row) for row in enhanced_rows.itertuples(index=False)] 
        columns = list(enhanced_rows.columns)
        pg_hook.insert_rows(
            table = 'warehouse.int_resale_prices',
            rows = records,
            target_fields = columns,
            commit_every = 500
        )
        print("Inserted enhanced data into warehouse.int_resale_prices\n")
        print(pd.DataFrame(records))
    
    @task
    def get_mrts_within_radius():
        pg_hook = PostgresHook("resale_price_db")
        # Fetch the recent resale price entries
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT rp.id, rp.latitude, rp.longitude
            FROM warehouse.int_resale_prices rp
            LEFT JOIN warehouse.int_nearest_mrt np 
            ON rp.id = np.flat_id
            WHERE np.flat_id IS NULL;
        """)

        mrts_df = pg_hook.get_pandas_df("""
            SELECT id, mrt, latitude, longitude
            FROM warehouse.int_mrts;
        """)

        for _, flat in resale_prices_df.iterrows():
            count_mrts = 0
            for _, mrt in mrts_df.iterrows():
                distance = calc_dist((flat['latitude'], flat['longitude']), (mrt['latitude'], mrt['longitude']))
                min_dist = None
                if distance <= PROXIMITY_RADIUS:
                    count_mrts += 1
                    if min_dist is None or distance < min_dist:
                        min_dist = distance
                # Persist details of nearest mrt to this flat
                if min_dist:
                    pg_hook.run("""
                        INSERT INTO warehouse.int_nearest_mrt (flat_id, nearest_mrt_id, num_mrts_within_radius, distance)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (flat_id) DO UPDATE 
                        SET nearest_mrt_id = EXCLUDED.nearest_mrt_id, 
                            num_mrts_within_radius = EXCLUDED.num_mrts_within_radius, 
                            distance = EXCLUDED.distance;
                    """, parameters=(flat['id'], mrt['id'], count_mrts, min_dist))
        print("Inserted nearest MRT stations for new resale prices into warehouse.int_nearest_mrt")

    @task
    def get_dist_from_cbd():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE distance_from_cbd IS NULL;
        """)
        onemap_scraper = OnemapScraper({})
        cbd_location = onemap_scraper.scrape_address_postal_coords(CBD_LANDMARK_ADDRESS)
        for _, flat in resale_prices_df.iterrows():
            dist_from_cbd = calc_dist((float(cbd_location['latitude']), float(cbd_location['longitude'])), (flat['latitude'], flat['longitude']))
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET distance_from_cbd = %s
                WHERE id = %s
            """, parameters=[dist_from_cbd, flat['id']])
        print("Updated distance to CBD")

    @task
    def process_data():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT 
                rp.*, 
                mrts.mrt AS nearest_mrt, 
                nm.num_mrts_within_radius as num_mrts_within_radius,
                nm.distance AS dist_to_nearest_mrt
            FROM 
                warehouse.int_resale_prices rp
            LEFT JOIN warehouse.int_nearest_mrt as nm ON rp.id = nm.flat_id
            JOIN warehouse.int_mrts as mrts ON mrts.id = nm.nearest_mrt_id;
        """)
        # Clean and standardise data
        resale_prices_df = clean_resale_prices_for_visualisation(resale_prices_df)
        return resale_prices_df
    
    @task
    def generate_report(df):
        plot_default_features(df)
        plot_mrt_info(df)
        # Paste images in report
        return create_html_report()

    # image_mail = EmailOperator(
    #     task_id="email_report",
    #     to=['e0560270@u.nus.edu'],
    #     subject='Resale Price Report',
    #     html_content='{{ ti.xcom_pull(task_ids="generate_report") }}'
    #     # provide_context=True
    # )
       
    # Run tasks
    scrape_resale_prices_ = scrape_resale_prices()
    enhance_resale_price_coords_ = enhance_resale_price_coords(scrape_resale_prices_)
    get_mrts_within_radius_ = get_mrts_within_radius()
    get_dist_from_cbd_ = get_dist_from_cbd()
    processed_data = process_data()
    generate_report_ = generate_report(processed_data)
    # Pipeline order
    scrape_resale_prices_ >>  enhance_resale_price_coords_ >> get_mrts_within_radius_ >> get_dist_from_cbd_
    get_dist_from_cbd_ >> processed_data >>  generate_report_# >> image_mail

hdb_pipeline_dag = hdb_pipeline()
