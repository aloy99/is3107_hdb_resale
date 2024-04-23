from contextlib import closing

from airflow.decorators import  task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email_operator import EmailOperator

from reporting.utils import plot_default_features, plot_mrt_info, plot_pri_sch_info, plot_park_info, plot_supermarket_info, create_html_report
from models.linear_regression import fit_linear_regression
from models.random_forest import fit_random_forest
from data_preparation.utils import clean_resale_prices_for_ml


@task_group(group_id = 'ml_tasks')
def ml_tasks():
    @task
    def select_and_transform_ml_data():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT * FROM warehouse.int_resale_prices;
        """)
        # Clean and standardise data
        cleaned_df = clean_resale_prices_for_ml(resale_prices_df)  
        return cleaned_df
    
    # @task
    # def generate_report(df_set):
    #     plot_default_features(df_set['resale_prices'])
    #     plot_mrt_info(df_set['mrt_prices'])
    #     plot_pri_sch_info(df_set['pri_sch_prices'])
    #     plot_park_info(df_set['parks_prices'])
    #     plot_supermarket_info(df_set['supermarket_prices'])
    #     # Paste images in report
    #     return create_html_report()

    # image_mail = EmailOperator(
    #     task_id="email_report",
    #     to=['e0560270@u.nus.edu'],
    #     subject='Resale Price Report',
    #     html_content='{{ ti.xcom_pull(task_ids="generate_report") }}'
    #     # provide_context=True
    # )
    data = select_and_transform_ml_data()
    fit_linear_regression(data)
    fit_random_forest(data)
    #data >> report
    #report >> image_mail
