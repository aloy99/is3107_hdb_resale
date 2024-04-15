from contextlib import closing

from airflow.decorators import  task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email_operator import EmailOperator

from reporting.utils import plot_default_features, plot_mrt_info, plot_pri_sch_info, create_html_report
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
                rp.resale_price, 
                rp.floor_area_sqm,
                mrts.mrt AS nearest_mrt, 
                nm.num_mrts_within_radius as num_mrts_within_radius,
                nm.distance AS dist_to_nearest_mrt
            FROM 
                warehouse.int_resale_prices rp
            LEFT JOIN warehouse.int_nearest_mrt as nm ON rp.id = nm.flat_id
            JOIN warehouse.int_mrts as mrts ON mrts.id = nm.nearest_mrt_id;
        """)
        # pri_sch_prices_df = pg_hook.get_pandas_df("""
        #     SELECT 
        #         rp.resale_price, 
        #         rp.floor_area_sqm,
        #         prischs.*,
        #         nps.num_pri_sch_within_radius, 
        #         nps.nearest_distance   
        #     FROM 
        #         warehouse.int_resale_prices rp
        #     LEFT JOIN warehouse.int_nearest_pri_schools as nps ON rp.id = nps.flat_id
        #     JOIN warehouse.int_pri_schools as prischs ON prischs.id = nps.nearest_pri_sch_id;
        # """)

        all_dfs = {
            'resale_prices': resale_prices_df,
            'mrt_prices': mrt_prices_df,
            # 'nearest_pri_sch_prices': pri_sch_prices_df
        }
        # Clean and standardise data
        for key in all_dfs:
            all_dfs[key] = clean_resale_prices_for_visualisation(all_dfs[key])  
        return all_dfs
    
    @task
    def generate_report(df_set):
        plot_default_features(df_set['resale_prices'])
        plot_mrt_info(df_set['mrt_prices'])
        # plot_pri_sch_info(df_set['nearest_pri_sch_prices'])
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