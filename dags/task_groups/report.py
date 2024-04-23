from contextlib import closing

from airflow.decorators import  task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email_operator import EmailOperator

from reporting.utils import plot_default_features, plot_mrt_info, plot_pri_sch_info, plot_park_info, plot_supermarket_info, create_html_report
from data_preparation.utils import clean_resale_prices_for_visualisation


@task_group(group_id = 'report')
def report_tasks():
    @task
    def select_and_transform_report_data():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT * FROM warehouse.int_resale_prices;
        """)
        mrt_prices_df = pg_hook.get_pandas_df("""
            SELECT 
                rp.id as flat_id,
                mrts.id as mrt_id,
                rp.resale_price, 
                rp.floor_area_sqm,
                mrts.mrt AS mrt, 
                nm.distance AS distance_to_mrt
            FROM 
                warehouse.int_resale_prices rp
            JOIN warehouse.int_nearest_mrts as nm ON rp.id = nm.flat_id
            JOIN warehouse.int_mrts as mrts ON mrts.id = nm.mrt_id;
        """)
        pri_sch_prices_df = pg_hook.get_pandas_df("""
            SELECT
                rp.id as flat_id,
                rp.resale_price,
                rp.floor_area_sqm,
                ps.*,
                nps.pri_sch_id,
                nps.distance as distance_to_school
            FROM warehouse.int_resale_prices rp
            JOIN warehouse.int_nearest_pri_schools nps ON rp.id = nps.flat_id
            JOIN warehouse.int_pri_schools ps ON nps.pri_sch_id = ps.id;
        """)
        parks_df = pg_hook.get_pandas_df("""
            SELECT
                rp.id as flat_id,
                rp.resale_price,
                rp.floor_area_sqm,
                parks.*,
                np.distance as distance_to_park
            FROM warehouse.int_resale_prices rp
            JOIN warehouse.int_nearest_parks np ON rp.id = np.flat_id
            JOIN warehouse.int_parks parks ON np.park_id = parks.id;
        """)
        supermarkets_df = pg_hook.get_pandas_df("""
            SELECT
                rp.id as flat_id,
                rp.resale_price,
                rp.floor_area_sqm,
                supermarkets.id,
                np.distance as distance_to_supermarket
            FROM warehouse.int_resale_prices rp
            JOIN warehouse.int_nearest_supermarkets np ON rp.id = np.flat_id
            JOIN warehouse.int_supermarkets supermarkets ON np.supermarket_id = supermarkets.id;
        """)
        all_dfs = {
            'resale_prices': resale_prices_df,
            'mrt_prices': mrt_prices_df,
            'pri_sch_prices': pri_sch_prices_df,
            'parks_prices': parks_df,
            'supermarket_prices': supermarkets_df
        }
        # Clean and standardise data
        for key in all_dfs:
            all_dfs[key] = clean_resale_prices_for_visualisation(all_dfs[key])  
        return all_dfs
    
    @task
    def generate_report(df_set):
        plot_default_features(df_set['resale_prices'])
        plot_mrt_info(df_set['mrt_prices'])
        plot_pri_sch_info(df_set['pri_sch_prices'])
        plot_park_info(df_set['parks_prices'])
        plot_supermarket_info(df_set['supermarket_prices'])
        # Paste images in report
        return create_html_report()

    image_mail = EmailOperator(
        task_id="email_report",
        to=['e0560270@u.nus.edu'],
        subject='Resale Price Report',
        html_content='{{ ti.xcom_pull(task_ids="report.generate_report") }}'
        # provide_context=True
    )
    data = select_and_transform_report_data()
    report = generate_report(data)
    data >> report >> image_mail
