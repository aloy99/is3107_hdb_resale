from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd

from scraper.datagov.park_scraper import ParkScraper
from scraper.onemap.onemap_scraper import OnemapScraper
from scraper.datagov.constants import PRIMARY_SCHOOL_FIELDS

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='parks_pipeline', default_args=default_args, schedule=None, catchup=False, tags=['parks_dag'], template_searchpath=["/opt/airflow/"])
def pri_school_pipeline():

    @task
    def scrape_parks():
        park_scraper = ParkScraper({})
        pg_hook = PostgresHook("resale_price_db")
        park_rows = park_scraper.get_parks()
        for park in park_rows:
            pg_hook.run("""
                INSERT INTO staging.stg_parks (park, latitude, longitude)
                VALUES (%s, %s, %s) ON CONFLICT (park) DO NOTHING;
            """, parameters=(park['park'], park['latitude'], park['longitude']))

    # Run tasks
    scrape_parks_ = scrape_parks()
    # Pipeline order
    scrape_parks_

pri_school_pipeline_dag = pri_school_pipeline()
