from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd

from common.constants import DEV_MODE, DEV_REDUCED_ROWS
from scraper.datagov.pri_school_scraper import PriSchoolScraper
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

@dag(dag_id='pri_schools_pipeline', default_args=default_args, schedule=None, catchup=False, tags=['pri_schools_dag'], template_searchpath=["/opt/airflow/"])
def pri_school_pipeline():

    create_pg_stg_schema = PostgresOperator(
        task_id = "create_pg_stg_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS staging;"
    )

    create_stg_pri_schools = PostgresOperator(
        task_id = "create_stg_pri_schools",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_pri_schools.sql"
    )

    @task
    def scrape_pri_schools():
        resale_price_scraper = PriSchoolScraper({})
        pg_hook = PostgresHook("resale_price_db")
        first_id = None
        for _, rows in enumerate(resale_price_scraper.run_scrape(), start=0):
            # necessary to support execute + commit + fetch, pg_hook doesn't support this combination let alone PostgresOperator
            with closing(pg_hook.get_conn()) as conn:
                if pg_hook.supports_autocommit:
                    pg_hook.set_autocommit(conn, True)
                with closing(conn.cursor()) as cursor:
                    column_names = ", ".join(PRIMARY_SCHOOL_FIELDS)
                    placeholders = ", ".join(["%s"] * len(PRIMARY_SCHOOL_FIELDS))
                    values_placeholder = ", ".join(["({})".format(placeholders)] * len(rows))
                    sql_statement = """
                        INSERT INTO staging.stg_pri_schools ({})
                        VALUES {}
                        ON CONFLICT (school_name) DO UPDATE SET
                        nature_code = EXCLUDED.nature_code,
                        sap_ind = EXCLUDED.sap_ind,
                        autonomous_ind = EXCLUDED.autonomous_ind,
                        gifted_ind = EXCLUDED.gifted_ind
                        RETURNING id;
                    """.format(column_names, values_placeholder)
                    cursor.execute(sql_statement, [val for row in rows for val in row])
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
        sql = "sql/tables/int_pri_schools.sql"
    )

    @task
    def enhance_pri_school_coords(min_id: int):
        if not min_id:
            return
        onemap_scraper = OnemapScraper({})
        pg_hook = PostgresHook("resale_price_db")
        sql_query = '''
            SELECT *
            FROM staging.stg_pri_schools
            WHERE id >= {}
        '''
        new_rows = pg_hook.get_pandas_df(
            sql = sql_query.format(min_id)
        )
        enhanced_rows = onemap_scraper.enhance_pri_school(new_rows)
        # Get location data
        enhanced_rows['latitude'] = pd.to_numeric(enhanced_rows['latitude'], errors='coerce')
        enhanced_rows['longitude'] = pd.to_numeric(enhanced_rows['longitude'], errors='coerce')
        # Drop rows without Location data and exclude 'postal' column
        enhanced_rows = enhanced_rows.loc[enhanced_rows['latitude'].notna() & enhanced_rows['longitude'].notna(), enhanced_rows.columns.difference(['postal'])]
        # Persist to data warehouse
        records = [list(row) for row in enhanced_rows.itertuples(index=False)] 
        columns = list(enhanced_rows.columns)
        pg_hook.insert_rows(
            table = 'warehouse.int_pri_schools',
            rows = records,
            target_fields = columns,
            commit_every = 500
        )
        print("Inserted enhanced data into warehouse.int_pri_schools\n")
        print(pd.DataFrame(records))
    
    # Run tasks
    scrape_parks_ = scrape_pri_schools()
    enhance_resale_price_coords_ = enhance_pri_school_coords(scrape_parks_)
    # Pipeline order
    create_pg_stg_schema >> create_stg_pri_schools >> scrape_parks_
    scrape_parks_ >> create_pg_warehouse_schema >> create_int_resale_price 
    create_int_resale_price >> enhance_resale_price_coords_ 

pri_school_pipeline_dag = pri_school_pipeline()
