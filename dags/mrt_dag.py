from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    @task
    def scrape_mrt_opening_dates():
        onemap_scraper = OnemapScraper({})
        mrts_df = get_mrt_location(onemap_scraper)
        print("Retrieved MRT location data\n", mrts_df)
    
    scrape_mrt_opening_dates_ = scrape_mrt_opening_dates()
    scrape_mrt_opening_dates_

mrt_pipeline_dag = mrt_pipeline()
