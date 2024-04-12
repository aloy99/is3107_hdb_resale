from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pandas as pd

from common.columns import TABLE_META
from migration_task_group import migration
from mrt_task_group import mrt_init

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='init_pipeline', default_args=default_args, schedule=None, catchup=False, tags=['main_dag'], template_searchpath=["/opt/airflow/"])
def init_pipeline():

    migration() >> mrt_init()

init_pipeline_dag = init_pipeline()
