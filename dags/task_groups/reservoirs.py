from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd

from scraper.arcgis.reservoir_scraper import ReservoirScraper

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@task_group(group_id = "reservoirs")
def reservoirs_tasks():
    @task
    def scrape_reservoirs():
        reservoir_scraper = ReservoirScraper({})
        pg_hook = PostgresHook("resale_price_db")
        reservoir_rows = reservoir_scraper.get_reservoirs()
        for reservoir in reservoir_rows:
            pg_hook.run("""
                INSERT INTO staging.stg_reservoirs (reservoir, latitude, longitude)
                VALUES (%s, %s, %s) ON CONFLICT (reservoir) DO NOTHING;
            """, parameters=(reservoir['name'], reservoir['lat'], reservoir['long']))
        return

    # Run tasks
    scrape_reservoirs_ = scrape_reservoirs()
    # Pipeline order
    scrape_reservoirs_
