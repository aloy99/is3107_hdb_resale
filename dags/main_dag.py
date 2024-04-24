from contextlib import closing
from datetime import datetime, timedelta
from functools import partial
import logging

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


import pandas as pd
import numpy as np
import dask.dataframe as dd

from common.columns import TABLE_META
from common.constants import CBD_LANDMARK_ADDRESS, FETCHING_RADIUS
from common.utils import calc_dist, process_amenities_ball_tree
from scraper.datagov.resale_price_scraper import ResalePriceScraper
from scraper.onemap.onemap_scraper import OnemapScraper
from scraper.singstat.singstat_scraper import SingstatScraper

from task_groups.report import report_tasks
from task_groups.ml import ml_tasks

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='daily_pipeline_dag', default_args=default_args, concurrency = 2, schedule=None, catchup=False, tags=['main_dag'], template_searchpath=["/opt/airflow/"])
def hdb_pipeline():

    @task
    def scrape_resale_prices():
        context = get_current_context()
        date = context["execution_date"]
        resale_price_scraper = ResalePriceScraper({}, "live") # use `backfill` for all data and `live` to only scrape latest dataset
        pg_hook = PostgresHook("resale_price_db")
        first_id = None
        for _, df in enumerate(resale_price_scraper.run_scrape(date), start=0):
            for k,g in df.groupby(np.arange(len(df))//100):
                with closing(pg_hook.get_conn()) as conn:
                    if pg_hook.supports_autocommit:
                        pg_hook.set_autocommit(conn, True)
                    with closing(conn.cursor()) as cursor:
                        cursor.execute(
                            f"""
                            INSERT INTO staging.stg_resale_prices ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id'])})
                            VALUES {",".join(["({})".format(",".join(['%s'] * (len(TABLE_META['stg_resale_prices'].columns)-1)))] * g.shape[0])}
                            ON CONFLICT ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id' and col != 'remaining_lease'])}) DO NOTHING
                            RETURNING id;
                            """,
                            [x for row in g.values for x in row]
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
        logger.info(f"Percentage of rows without successful match in OneMap: {enhanced_rows['latitude'].isnull().sum() * 100 / len(enhanced_rows)}")
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
    def calculate_real_prices():
        pg_hook = PostgresHook("resale_price_db")
        # Exit gracefully if there are no preexisting data
        records_exist = pg_hook.get_first("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'warehouse' AND table_name = 'int_resale_prices');")
        if not records_exist or not records_exist[0]:
            print("No records exist!")
            return
        # Fetch the recent resale price entries
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT id, transaction_month, resale_price
            FROM warehouse.int_resale_prices
            WHERE real_resale_price IS NULL;
        """)
        # Join with latest CPI data
        singstat_scraper = SingstatScraper({})
        cpi_data = singstat_scraper.load_cpi_data()
        cpi_prices_df = resale_prices_df.merge(cpi_data, left_on='transaction_month', right_on='month', how='inner')
        cpi_prices_df['real_resale_price'] = (cpi_prices_df['resale_price'] / cpi_prices_df['cpi']) * 100
        # Update db
        for _, row in cpi_prices_df.iterrows():
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET real_resale_price = %s
                WHERE id = %s;
                """, 
                parameters=(row['real_resale_price'], row['id'])
            )
        print("Calculated real prices")
    
    @task
    def get_mrts_within_radius():
        pg_hook = PostgresHook("resale_price_db")

        resale_prices_df = dd.from_pandas(pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_mrt_within_radius IS NULL;                                                    
        """), chunksize = 5000)
        mrt_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_mrts;")
        meta_df = pd.DataFrame({
            'flat_id': pd.Series(dtype = 'int'),
            'count': pd.Series(dtype = 'int'),
            'nearest_amenities': pd.Series(dtype = 'object'),
            'distances': pd.Series(dtype = 'object')
        })
        res = resale_prices_df.map_partitions(partial(process_amenities_ball_tree, amenity_df = mrt_df), meta=meta_df)
        res = res.compute()
        res.explode(['nearest_amenities', 'distances']).reset_index(drop=True)[['flat_id','nearest_amenities','distances']].to_csv('testing.csv')
        nearest_amenities = res.explode(['nearest_amenities', 'distances']).reset_index(drop=True)[['flat_id','nearest_amenities','distances']].dropna().to_numpy().tolist()
        
        pg_hook.insert_rows(
                table = 'warehouse.int_nearest_mrts',
                rows = nearest_amenities,
                target_fields = ['flat_id', 'mrt_id', 'distance'],
                commit_every = 500
            )
        for _, row in res.iterrows():
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_mrt_within_radius = %s
                WHERE id = %s; 
            """, parameters=(row['count'], row['flat_id']))
            

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
        resale_prices_df = dd.from_pandas(pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_pri_sch_within_radius IS NULL;
        """), chunksize = 5000)
        pri_school_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_pri_schools;")

        meta_df = pd.DataFrame({
            'flat_id': pd.Series(dtype = 'int'),
            'count': pd.Series(dtype = 'int'),
            'nearest_amenities': pd.Series(dtype = 'object'),
            'distances': pd.Series(dtype = 'object')
        })
        res = resale_prices_df.map_partitions(partial(process_amenities_ball_tree, amenity_df = pri_school_df), meta=meta_df)
        res = res.compute()
        nearest_amenities = res.explode(['nearest_amenities', 'distances']).reset_index(drop=True)[['flat_id','nearest_amenities','distances']].dropna().to_numpy().tolist()
        pg_hook.insert_rows(
            table = 'warehouse.int_nearest_pri_schools',
            rows = nearest_amenities,
            target_fields = ['flat_id', 'pri_sch_id', 'distance'],
            commit_every = 500
        )
        for _, row in res.iterrows():
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_pri_sch_within_radius = %s
                WHERE id = %s; 
            """, parameters=(row['count'], row['flat_id']))

    @task
    def get_parks_within_radius():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = dd.from_pandas(pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_park_within_radius IS NULL;
        """), chunksize = 5000)
        park_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_parks;")
        meta_df = pd.DataFrame({
            'flat_id': pd.Series(dtype = 'int'),
            'count': pd.Series(dtype = 'int'),
            'nearest_amenities': pd.Series(dtype = 'object'),
            'distances': pd.Series(dtype = 'object')
        })
        res = resale_prices_df.map_partitions(partial(process_amenities_ball_tree, amenity_df = park_df), meta=meta_df)
        res = res.compute()
        nearest_amenities = res.explode(['nearest_amenities', 'distances']).reset_index(drop=True)[['flat_id','nearest_amenities','distances']].dropna().to_numpy().tolist()
        pg_hook.insert_rows(
            table = 'warehouse.int_nearest_parks',
            rows = nearest_amenities,
            target_fields = ['flat_id', 'park_id', 'distance'],
            commit_every = 500
        )
        # Batch updates
        for _, row in res.iterrows():
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_park_within_radius = %s
                WHERE id = %s; 
            """, parameters=(row['count'], row['flat_id']))
    @task
    def get_supermarkets_within_radius():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = dd.from_pandas(pg_hook.get_pandas_df("""
            SELECT id, latitude, longitude
            FROM warehouse.int_resale_prices
            WHERE num_supermarkets_within_radius IS NULL;
        """), chunksize = 5000)
        supermarket_df = pg_hook.get_pandas_df("SELECT * FROM warehouse.int_supermarkets;")
        meta_df = pd.DataFrame({
            'flat_id': pd.Series(dtype = 'int'),
            'count': pd.Series(dtype = 'int'),
            'nearest_amenities': pd.Series(dtype = 'object'),
            'distances': pd.Series(dtype = 'object')
        })
        res = resale_prices_df.map_partitions(partial(process_amenities_ball_tree, amenity_df = supermarket_df), meta=meta_df)
        res = res.compute()
        nearest_amenities = res.explode(['nearest_amenities', 'distances']).reset_index(drop=True)[['flat_id','nearest_amenities','distances']].dropna().to_numpy().tolist()
        pg_hook.insert_rows(
            table = 'warehouse.int_nearest_supermarkets',
            rows = nearest_amenities,
            target_fields = ['flat_id', 'supermarket_id', 'distance'],
            commit_every = 500
        )
        # Batch updates
        for _, row in res.iterrows():
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET num_supermarkets_within_radius = %s
                WHERE id = %s; 
            """, parameters=(row['count'], row['flat_id']))
            
    # Run tasks
    scrape_resale_prices_ = scrape_resale_prices()
    enhance_resale_price_coords_ = enhance_resale_price_coords(scrape_resale_prices_)
    calculate_real_prices_ = calculate_real_prices()
    get_mrts_within_radius_ = get_mrts_within_radius()
    get_pri_schs_within_radius_ = get_pri_schs_within_radius()
    get_parks_within_radius_ = get_parks_within_radius()
    get_supermarkets_within_radius_ = get_supermarkets_within_radius()
    get_dist_from_cbd_ = get_dist_from_cbd()

    # Pipeline order
    scrape_resale_prices_ >>  enhance_resale_price_coords_ >> calculate_real_prices_ >> get_mrts_within_radius_  >> get_pri_schs_within_radius_ >> get_parks_within_radius_ >> get_supermarkets_within_radius_ >> get_dist_from_cbd_
    get_dist_from_cbd_ >> ml_tasks() >> report_tasks()

hdb_pipeline_dag = hdb_pipeline()
