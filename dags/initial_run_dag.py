from contextlib import closing
from datetime import datetime, timedelta

from airflow.decorators import dag

from task_groups.migration import migration_tasks
from task_groups.mrt import mrt_tasks
from task_groups.park import park_tasks
from task_groups.pri_schools import pri_school_tasks
from task_groups.reservoirs import reservoirs_tasks

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
    migration_tasks() >> [mrt_tasks(), park_tasks(), pri_school_tasks(), reservoirs_tasks()]
    

initial_setup_dag = initial_setup()
