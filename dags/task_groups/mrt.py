from contextlib import closing
from common.columns import TABLE_META

from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scraper.onemap.onemap_scraper import OnemapScraper
from scraper.amenities.mrt_scraper import get_mrt_opening_dates, get_mrts_location

@task_group(group_id = 'mrt')
def mrt_tasks():
    @task
    def scrape_mrt_data():
        mrts_df = get_mrt_opening_dates()
        print("Retrieved MRT location data\n", mrts_df)
        # persist to staging db
        pg_hook = PostgresHook("resale_price_db")
        insert_stmt = """
        INSERT INTO staging.stg_mrts (mrt, opening_date)
        VALUES (%s, %s) ON CONFLICT (mrt) DO NOTHING;
        """
        with closing(pg_hook.get_conn()) as conn:
            with conn.cursor() as cur:
                for _, row in mrts_df.iterrows():
                    cur.execute(insert_stmt, (row['mrt'], row['opening_date']))
                conn.commit()
        print("committed mrt data into warehouse")        
        return mrts_df
       
    @task 
    def scrape_mrt_location_data(mrt_opening_data):
        onemap_scraper = OnemapScraper({})
        mrts_df = get_mrts_location(onemap_scraper, mrt_opening_data)
        pg_hook = PostgresHook("resale_price_db")
        insert_stmt = """
        INSERT INTO warehouse.int_mrts (mrt, opening_date, latitude, longitude)
        VALUES (%s, %s, %s, %s) ON CONFLICT (mrt) DO NOTHING;
        """
        with closing(pg_hook.get_conn()) as conn:
            with conn.cursor() as cur:
                for _, row in mrts_df.iterrows():
                    cur.execute(insert_stmt, (row['mrt'], row['opening_date'], row['latitude'], row['longitude']))
                conn.commit()
        print("committed mrt data into warehouse")
        
    scrape_mrt_data_ = scrape_mrt_data()
    scrape_mrt_data_ >> scrape_mrt_location_data(scrape_mrt_data_)
