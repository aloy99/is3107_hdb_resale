from contextlib import closing

from airflow.decorators import  task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email_operator import EmailOperator

from reporting.utils import plot_default_features, plot_mrt_info, create_html_report
from data_preparation.utils import clean_resale_prices_for_visualisation


@task_group(group_id = 'report')
def report_tasks():
    @task
    def select_and_transform_report_data():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT 
                rp.*, 
                mrts.mrt AS nearest_mrt, 
                nm.num_mrts_within_radius as num_mrts_within_radius,
                nm.distance AS dist_to_nearest_mrt
            FROM 
                warehouse.int_resale_prices rp
            LEFT JOIN warehouse.int_nearest_mrt as nm ON rp.id = nm.flat_id
            JOIN warehouse.int_mrts as mrts ON mrts.id = nm.nearest_mrt_id;
        """)
        # Clean and standardise data
        resale_prices_df = clean_resale_prices_for_visualisation(resale_prices_df)
        return resale_prices_df
    
    @task
    def generate_report(df):
        plot_default_features(df)
        plot_mrt_info(df)
        # Paste images in report
        return create_html_report()

    # image_mail = EmailOperator(
    #     task_id="email_report",
    #     to=['e0560270@u.nus.edu'],
    #     subject='Resale Price Report',
    #     html_content='{{ ti.xcom_pull(task_ids="generate_report") }}'
    #     # provide_context=True
    # )
    data = select_and_transform_report_data()
    report = generate_report(data)
    data >> report
    #report >> image_mail
