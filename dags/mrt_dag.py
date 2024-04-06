from contextlib import closing
from columns import TABLE_META

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scraper.onemap.onemap_scraper import OnemapScraper
from scraper.amenities.mrt_scraper import get_mrt_location

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='mrt_pipeline', default_args=default_args, schedule=None, catchup=False, tags=['mrt_dag'], template_searchpath=["/opt/airflow/"])
def mrt_pipeline():

    create_pg_stg_schema = PostgresOperator(
        task_id = "create_pg_stg_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS staging;"
    )

    create_stg_resale_price = PostgresOperator(
        task_id = "create_stg_resale_price",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_mrts.sql"
    )

    @task
    def scrape_mrt_opening_dates():
        onemap_scraper = OnemapScraper({})
        mrts_df = get_mrt_location(onemap_scraper)
        print("Retrieved MRT location data\n", mrts_df)
        # persist to staging db
        pg_hook = PostgresHook("resale_price_db")
        insert_stmt = """
        INSERT INTO staging.stg_mrts (mrt, opening_date, latitude, longitude)
        VALUES (%s, %s, %s, %s) ON CONFLICT (mrt) DO NOTHING;
        """
        with closing(pg_hook.get_conn()) as conn:
            with conn.cursor() as cur:
                for _, row in mrts_df.iterrows():
                    cur.execute(insert_stmt, (row['mrt'], row['opening_date'], row['latitude'], row['longitude']))
                conn.commit()
        print("committed mrt data into staging db")
    
    scrape_mrt_opening_dates_ = scrape_mrt_opening_dates()
    create_pg_stg_schema >> create_stg_resale_price >> scrape_mrt_opening_dates_

mrt_pipeline_dag = mrt_pipeline()
