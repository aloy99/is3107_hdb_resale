import os
import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd

from scraper.datagov.datagov_scraper import DataGovScraper

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}


@dag(dag_id='hdb_pipeline', default_args=default_args, schedule=None, catchup=False, tags=['main_dag'])
def hdb_pipeline():

    create_pg_schema = PostgresOperator(
        task_id = "create_pg_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS staging;"
    )

    create_stg_resale_price = PostgresOperator(
        task_id = "create_stg_resale_price",
        postgres_conn_id = "resale_price_db",
        sql = "sql/stg_resale_prices.sql"
    )

    @task
    def scrape_resale_prices():
        data_gov_scraper = DataGovScraper("", "", {}, "live")
        sql = '''
        INSERT INTO staging.stg_resale_prices
        VALUES(%s)'''
        pg_hook = PostgresHook("resale_price_db")
        for rows in data_gov_scraper.run_scrape():
            pg_hook.insert_rows("staging.stg_resale_prices", rows)

    create_pg_schema >> create_stg_resale_price >> scrape_resale_prices()


hdb_pipeline_dag = hdb_pipeline()
