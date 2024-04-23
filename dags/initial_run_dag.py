from contextlib import closing
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

from scraper.onemap.onemap_scraper import OnemapScraper
from scraper.datagov.resale_price_scraper import ResalePriceScraper

from common.columns import TABLE_META

from task_groups.migration import migration_tasks
from task_groups.mrt import mrt_tasks
from task_groups.park import park_tasks
from task_groups.pri_schools import pri_school_tasks
from task_groups.supermarket import supermarket_tasks
from task_groups.reservoirs import reservoirs_tasks

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

@dag(dag_id='initial_setup_dag', default_args=default_args, schedule=None, catchup=False, tags=['migration'], template_searchpath=["/opt/airflow/"])
def initial_setup():

    @task
    def scrape_resale_prices():
        context = get_current_context()
        date = context["execution_date"]
        resale_price_scraper = ResalePriceScraper({}, "backfill") # use `backfill` for all data and `live` to only scrape latest dataset
        pg_hook = PostgresHook("resale_price_db")
        first_id = None
        for _, df in enumerate(resale_price_scraper.run_scrape(date), start=0):
            for _, row in df.iterrows():
                with closing(pg_hook.get_conn()) as conn:
                    if pg_hook.supports_autocommit:
                        pg_hook.set_autocommit(conn, True)
                    with closing(conn.cursor()) as cursor:
                        cursor.execute(
                            f"""
                            INSERT INTO staging.stg_resale_prices ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id'])})
                            VALUES {",".join(["({})".format(",".join(['%s'] * (len(TABLE_META['stg_resale_prices'].columns)-1)))])}
                            ON CONFLICT ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id' and col != 'remaining_lease'])}) DO NOTHING
                            RETURNING id;
                            """,
                            row.values
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

    scrape_resale_prices_ = scrape_resale_prices()
    migration_tasks() >> [mrt_tasks(), park_tasks(), pri_school_tasks(), supermarket_tasks(), reservoirs_tasks()] >> scrape_resale_prices_ >> enhance_resale_price_coords(scrape_resale_prices_)
    

initial_setup_dag = initial_setup()
