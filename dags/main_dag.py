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
        data_gov_scraper = DataGovScraper({}, "backfill")
        pg_hook = PostgresHook("resale_price_db")
        for rows in data_gov_scraper.run_scrape(date):
            first_id = None
            #necessary to support execute + commit + fetch, pg_hook doesn't support this combination let alone PostgresOperator
            with closing(pg_hook.get_conn()) as conn:
                if pg_hook.supports_autocommit:
                    pg_hook.set_autocommit(conn, True)
                with closing(conn.cursor()) as cursor:
                    print(f"""
                        INSERT INTO staging.stg_resale_prices ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id'])})
                        VALUES {",".join(["({})".format(",".join(['%s'] * (len(TABLE_META['stg_resale_prices'].columns)-1)))]*len(rows))}
                        RETURNING id;
                        """)
                    cursor.execute(
                        f"""
                        INSERT INTO staging.stg_resale_prices ({",".join([col for col in TABLE_META['stg_resale_prices'].columns if col != 'id'])})
                        VALUES {",".join(["({})".format(",".join(['%s'] * (len(TABLE_META['stg_resale_prices'].columns)-1)))]*len(rows))}
                        ON CONFLICT DO NOTHING
                        RETURNING id;
                        """,
                        [val for row in rows for val in row]
                    )
                    curr_id = cursor.fetchone()
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
        
    # select_new_resale_price_rows = PostgresOperator(
    #     task_id = "enhance_resale_price_coords_select",
    #     postgres_conn_id = "resale_price_db",
    #     sql = "enhance_resale_price_coords_select.sql",
    #     # params = {'min_id': "{{ task_instance.xcom_pull(task_ids='scrape_resale_prices') }}"}
    # )
    @task
    def enhance_resale_price_coords(min_id: int):
        onemap_scraper = OnemapScraper({})
        pg_hook = PostgresHook("resale_price_db")
        with open("/opt/airflow/dags/enhance_resale_price_coords_select.sql", 'r') as file:
            sql_query = file.read()
            new_rows = pg_hook.get_pandas_df(
                sql = sql_query.format(min_id)
            )

        enhanced_rows = onemap_scraper.enhance_resale_price(new_rows)
        records = [list(row) for row in enhanced_rows.itertuples(index=False)] 
        #records = enhanced_rows.to_records(index=False)
        columns = list(enhanced_rows.columns)

        pg_hook.insert_rows(
            table = 'warehouse.int_resale_prices',
            rows = records,
            target_fields = columns,
            commit_every = 500
        )

        # resale_price_new_rows = PostgresOperator(
        #     task_id = "enhance_resale_price_coords_select",
        #     postgres_conn_id = "resale_price_db",
        #     sql = "enhance_resale_price_coords_select.sql",
        #     params = {'min_id': min_id}
        # )
        # new_rows = resale_price_new_rows.execute({})
        
        # pg_hook = PostgresHook("resale_price_db")
        # pg_hook.insert_rows(
        #     table='warehouse.int_resale_prices',
        #     rows = enhanced_rows.to_dict()
        # )

        #select from stg_resale_prices where obs_time > prev

        
    scrape_resale_prices_ = scrape_resale_prices()
    create_pg_stg_schema >> create_stg_resale_price >> scrape_resale_prices_
    
    scrape_resale_prices_ >> create_pg_warehouse_schema >> create_int_resale_price >> enhance_resale_price_coords(62501)


hdb_pipeline_dag = hdb_pipeline()
