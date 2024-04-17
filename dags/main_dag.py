from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from common.columns import TABLE_META
from common.constants import DEV_MODE, DEV_REDUCED_ROWS, CBD_LANDMARK_ADDRESS, FETCHING_RADIUS
from common.utils import calc_dist, process_amenities
from scraper.datagov.resale_price_scraper import ResalePriceScraper
from scraper.onemap.onemap_scraper import OnemapScraper

from task_groups.report import report_tasks

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

        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_mrt_within_radius IS NULL;
        """)
        pri_school_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_mrts;")
        results = []
        with ThreadPoolExecutor(10) as executor:
            future_to_flat = {executor.submit(process_amenities, flat, pri_school_df): flat for _, flat in resale_prices_df.iterrows()}
            for future in as_completed(future_to_flat):
                results.append(future.result())
        insert_params = []
        update_params = []
        for result in results:
            insert_params.extend([(res['flat_id'], res['amenity_id'], res['distance']) for res in result['nearest_amenities']])
            update_params.append((result['count'], result['flat_id']))
        # Batch insertions
        if insert_params:
            pg_hook.insert_rows(
                table = 'warehouse.int_nearest_mrts',
                rows = insert_params,
                target_fields = ['flat_id', 'mrt_id', 'distance'],
                commit_every = 500
            )
        # Batch updates
        for count, flat_id in update_params:
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_mrt_within_radius = %s
                WHERE id = %s; 
            """, parameters=(count, flat_id))

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
    def get_pri_schs_within_radius():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_pri_sch_within_radius IS NULL;
        """)
        pri_school_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_pri_schools;")
        results = []
        with ThreadPoolExecutor(10) as executor:
            future_to_flat = {executor.submit(process_amenities, flat, pri_school_df): flat for _, flat in resale_prices_df.iterrows()}
            for future in as_completed(future_to_flat):
                results.append(future.result())
        insert_params = []
        update_params = []
        for result in results:
            insert_params.extend([(res['flat_id'], res['amenity_id'], res['distance']) for res in result['nearest_amenities']])
            update_params.append((result['count'], result['flat_id']))
        # Batch insertions
        if insert_params:
            pg_hook.insert_rows(
                table = 'warehouse.int_nearest_pri_schools',
                rows = insert_params,
                target_fields = ['flat_id', 'pri_sch_id', 'distance'],
                commit_every = 500
            )
        # Batch updates
        for count, flat_id in update_params:
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_pri_sch_within_radius = %s
                WHERE id = %s; 
            """, parameters=(count, flat_id))

    @task
    def get_parks_within_radius():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_park_within_radius IS NULL;
        """)
        park_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_parks;")
        results = []
        with ThreadPoolExecutor(10) as executor:
            future_to_flat = {executor.submit(process_amenities, flat, park_df): flat for _, flat in resale_prices_df.iterrows()}
            for future in as_completed(future_to_flat):
                results.append(future.result())
        insert_params = []
        update_params = []
        for result in results:
            insert_params.extend([(res['flat_id'], res['amenity_id'], res['distance']) for res in result['nearest_amenities']])
            update_params.append((result['count'], result['flat_id']))
        # Batch insertions
        if insert_params:
            pg_hook.insert_rows(
                table = 'warehouse.int_nearest_parks',
                rows = insert_params,
                target_fields = ['flat_id', 'park_id', 'distance'],
                commit_every = 500
            )
        # Batch updates
        for count, flat_id in update_params:
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_park_within_radius = %s
                WHERE id = %s; 
            """, parameters=(count, flat_id))
    @task
    def get_supermarkets_within_radius():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_supermarkets_within_radius IS NULL;
        """)
        supermarket_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_supermarkets;")
        results = []
        with ThreadPoolExecutor(10) as executor:
            future_to_flat = {executor.submit(process_amenities, flat, supermarket_df): flat for _, flat in resale_prices_df.iterrows()}
            for future in as_completed(future_to_flat):
                results.append(future.result())
        insert_params = []
        update_params = []
        for result in results:
            insert_params.extend([(res['flat_id'], res['amenity_id'], res['distance']) for res in result['nearest_amenities']])
            update_params.append((result['count'], result['flat_id']))
        # Batch insertions
        if insert_params:
            pg_hook.insert_rows(
                table = 'warehouse.int_nearest_supermarkets',
                rows = insert_params,
                target_fields = ['flat_id', 'supermarket_id', 'distance'],
                commit_every = 500
            )
        # Batch updates
        for count, flat_id in update_params:
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_supermarkets_within_radius = %s
                WHERE id = %s; 
            """, parameters=(count, flat_id))
            
    # Run tasks
    scrape_resale_prices_ = scrape_resale_prices()
    enhance_resale_price_coords_ = enhance_resale_price_coords(scrape_resale_prices_)
    get_mrts_within_radius_ = get_mrts_within_radius()
    get_pri_schs_within_radius_ = get_pri_schs_within_radius()
    get_parks_within_radius_ = get_parks_within_radius()
    get_supermarkets_within_radius_ = get_supermarkets_within_radius()
    get_dist_from_cbd_ = get_dist_from_cbd()

    # Pipeline order
    scrape_resale_prices_ >>  enhance_resale_price_coords_ >> get_mrts_within_radius_ >> get_pri_schs_within_radius_ >> get_parks_within_radius_ >> get_supermarkets_within_radius_ >> get_dist_from_cbd_
    get_dist_from_cbd_ >> report_tasks()

hdb_pipeline_dag = hdb_pipeline()
