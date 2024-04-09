import os
import json
from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd

from columns import TABLE_META
from constants import DEV_MODE, DEV_REDUCED_ROWS
from scraper.datagov.datagov_scraper import DataGovScraper
from scraper.onemap.onemap_scraper import OnemapScraper
from transformations.enhance_resale_price import enhance_resale_price

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

    @task
    def scrape_resale_prices():
        context = get_current_context()
        date = context["execution_date"]
        data_gov_scraper = DataGovScraper({}, "live") # use `backfill` for all data and `live` to only scrape latest dataset
        pg_hook = PostgresHook("resale_price_db")
        first_id = None
        for idx, rows in enumerate(data_gov_scraper.run_scrape(date), start=0):
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
                        ON CONFLICT ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id'])}) DO NOTHING
                        RETURNING id;
                        """,
                        [val for row in rows for val in row]
                    )
                    curr_id = cursor.fetchone()
                    if curr_id:
                        first_id = first_id if first_id else curr_id
                        first_id = min(first_id, curr_id)
        return first_id[0] if first_id else first_id
        

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
        # Add location data
        enhanced_rows = onemap_scraper.enhance_resale_price(new_rows)
        # Remove rows without location data
        filtered_rows = enhanced_rows[enhanced_rows['postal'].notnull() and enhanced_rows['postal'] != 'NIL']
        records = [list(row) for row in filtered_rows.itertuples(index=False)] 
        columns = list(enhanced_rows.columns)
        # Persist to data warehouse
        pg_hook.insert_rows(
            table = 'warehouse.int_resale_prices',
            rows = records,
            target_fields = columns,
            commit_every = 500
        )
        print("Inserted enhanced data into warehouse.int_resale_prices\n")
        print(pd.DataFrame(records))

    scrape_resale_prices_ = scrape_resale_prices()
    create_pg_stg_schema >> create_stg_resale_price >> scrape_resale_prices_
    
    scrape_resale_prices_ >> create_pg_warehouse_schema >> create_int_resale_price >> enhance_resale_price_coords(scrape_resale_prices_)


hdb_pipeline_dag = hdb_pipeline()
