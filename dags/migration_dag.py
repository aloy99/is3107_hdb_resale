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

@dag(dag_id='migration', default_args=default_args, schedule=None, catchup=False, tags=['migration'], template_searchpath=["/opt/airflow/"])
def migration():

    create_pg_stg_schema = PostgresOperator(
        task_id = "create_pg_stg_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS staging;"
    )

    create_stg_resale_price = PostgresOperator(
        task_id = "create_stg_resale_price",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_resale_prices.sql"
    )

    create_stg_mrts = PostgresOperator(
        task_id = "create_stg_mrts",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_mrts.sql"
    )  

    create_stg_parks = PostgresOperator(
        task_id = "create_stg_parks",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_parks.sql"
    )

    create_stg_pri_schools = PostgresOperator(
        task_id = "create_stg_pri_schools",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_pri_schools.sql"
    )

    create_pg_warehouse_schema = PostgresOperator(
        task_id = "create_pg_warehouse_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS warehouse;"
    )

    create_int_resale_price = PostgresOperator(
        task_id = "create_int_resale_price",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_resale_prices.sql"
    )

    create_int_mrts = PostgresOperator(
        task_id = "create_int_mrts",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_mrts.sql"
    )

    create_int_nearest_mrt = PostgresOperator(
        task_id = "create_int_nearest_mrt",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_nearest_mrt.sql"
    )

    create_int_pri_schools = PostgresOperator(
        task_id = "create_int_pri_schools",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_pri_schools.sql"
    )

    create_pg_stg_schema >> [create_stg_resale_price, create_stg_mrts, create_stg_parks, create_stg_pri_schools]
    create_pg_warehouse_schema >> [create_int_mrts, create_int_nearest_mrt, create_int_resale_price, create_int_pri_schools] 

migration_dag = migration()
