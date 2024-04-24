import dask.dataframe as dd

from airflow.decorators import  task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email_operator import EmailOperator

from reporting.utils import plot_default_features, plot_mrt_info, plot_pri_sch_info, plot_park_info, plot_supermarket_info, create_html_report
from data_preparation.utils import clean_resale_prices_for_visualisation


@task_group(group_id='report')
def report_tasks():
    @task
    def process_resale_prices():
        pg_hook = PostgresHook("resale_price_db")
        resale_prices_df = pg_hook.get_pandas_df("""
            SELECT * FROM warehouse.int_resale_prices;
        """)
        resale_prices_df = clean_resale_prices_for_visualisation(resale_prices_df)
        plot_default_features(resale_prices_df)
        del resale_prices_df

    @task
    def process_mrt_prices():
        pg_hook = PostgresHook("resale_price_db")
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
        mrt_prices_df = clean_resale_prices_for_visualisation(mrt_prices_df)
        plot_mrt_info(mrt_prices_df)
        del mrt_prices_df

    @task
    def process_pri_sch_prices():
        pg_hook = PostgresHook("resale_price_db")
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
        pri_sch_prices_df = clean_resale_prices_for_visualisation(pri_sch_prices_df)
        plot_pri_sch_info(pri_sch_prices_df)
        del pri_sch_prices_df

    @task
    def process_parks_prices():
        pg_hook = PostgresHook("resale_price_db")
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
        parks_df = clean_resale_prices_for_visualisation(parks_df)
        plot_park_info(parks_df)
        del parks_df

    @task
    def process_supermarkets_prices():
        pg_hook = PostgresHook("resale_price_db")
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
        supermarkets_df = clean_resale_prices_for_visualisation(supermarkets_df)
        plot_supermarket_info(supermarkets_df)
        del supermarkets_df

    @task
    def generate_report():
        return create_html_report()

    process_resale_prices_ = process_resale_prices()
    process_mrt_prices_ = process_mrt_prices()
    process_pri_sch_prices_ = process_pri_sch_prices()
    process_parks_prices_ = process_parks_prices()
    process_supermarkets_prices_ = process_supermarkets_prices()
    report = generate_report()

    image_mail = EmailOperator(
        task_id="email_report",
        to=['e0560270@u.nus.edu'],
        subject='Resale Price Report',
        html_content='{{ ti.xcom_pull(task_ids="report.generate_report") }}'
    )

    [process_mrt_prices_, process_parks_prices_, process_pri_sch_prices_, process_resale_prices_, process_supermarkets_prices_] >> report

    report >> image_mail
