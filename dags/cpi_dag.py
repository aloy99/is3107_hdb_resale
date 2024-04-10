from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scraper.singstat.singstat_scraper import SingstatScraper

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='cpi_pipeline', default_args=default_args, schedule=None, catchup=False, tags=['cpi_dag'], template_searchpath=["/opt/airflow/"])
def cpi_pipeline():

    @task
    def calculate_real_prices():
        pg_hook = PostgresHook("resale_price_db")
        # Exit gracefully if there are no preexisting data
        records_exist = pg_hook.get_first("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'warehouse' AND table_name = 'int_resale_prices');")
        if not records_exist or not records_exist[0]:
            print("No records exist!")
            return
        # Fetch the recent resale price entries
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT id, transaction_month, resale_price
            FROM warehouse.int_resale_prices
            WHERE real_resale_price IS NULL;
        """)
        # Join with latest CPI data
        singstat_scraper = SingstatScraper({})
        cpi_data = singstat_scraper.load_cpi_data()
        cpi_prices_df = resale_prices_df.merge(cpi_data, left_on='transaction_month', right_on='month', how='inner')
        cpi_prices_df['real_resale_price'] = (cpi_prices_df['resale_price'] / cpi_prices_df['cpi']) * 100
        # Update db
        for _, row in cpi_prices_df.iterrows():
            pg_hook.run("""
                UPDATE warehouse.int_resale_prices
                SET real_resale_price = %s
                WHERE id = %s;
                """, 
                parameters=(row['real_resale_price'], row['id'])
            )
        print("Updated distance to CBD")

    calculate_real_prices() 

cpi_pipeline_dag = cpi_pipeline()
