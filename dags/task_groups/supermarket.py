from contextlib import closing

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd

from scraper.datagov.supermarket_scraper import SupermarketScraper
from scraper.onemap.onemap_scraper import OnemapScraper
from scraper.datagov.constants import SUPERMARKET_FIELDS

@task_group(group_id = "supermarket")
def supermarket_tasks():
    @task
    def scrape_supermarkets():
        supermarket_scraper = SupermarketScraper({})
        pg_hook = PostgresHook("resale_price_db")
        first_id = None
        for _, rows in enumerate(supermarket_scraper.run_scrape(), start=0):
            # necessary to support execute + commit + fetch, pg_hook doesn't support this combination let alone PostgresOperator
            with closing(pg_hook.get_conn()) as conn:
                if pg_hook.supports_autocommit:
                    pg_hook.set_autocommit(conn, True)
                with closing(conn.cursor()) as cursor:
                    column_names = ", ".join(SUPERMARKET_FIELDS)
                    placeholders = ", ".join(["%s"] * len(SUPERMARKET_FIELDS))
                    values_placeholder = ", ".join(["({})".format(placeholders)] * len(rows))
                    sql_statement = """
                        INSERT INTO staging.stg_supermarkets ({})
                        VALUES {}
                        ON CONFLICT (premise_address) DO UPDATE SET
                        business_name = EXCLUDED.business_name
                        RETURNING id;
                    """.format(column_names, values_placeholder)
                    cursor.execute(sql_statement, [val for row in rows for val in row])
                    curr_id = cursor.fetchone()
                    if curr_id:
                        first_id = first_id if first_id else curr_id
                        first_id = min(first_id, curr_id)
        return first_id[0] if first_id else first_id
        
    @task
    def enhance_supermarket_coords(min_id: int):
        if not min_id:
            return
        onemap_scraper = OnemapScraper({})
        pg_hook = PostgresHook("resale_price_db")
        sql_query = '''
            SELECT *
            FROM staging.stg_supermarkets
            WHERE id >= {}
        '''
        new_rows = pg_hook.get_pandas_df(
            sql = sql_query.format(min_id)
        )
        enhanced_rows = onemap_scraper.enhance_supermarket(new_rows)
        # Get location data
        enhanced_rows['latitude'] = pd.to_numeric(enhanced_rows['latitude'], errors='coerce')
        enhanced_rows['longitude'] = pd.to_numeric(enhanced_rows['longitude'], errors='coerce')
        # Drop rows without Location data and exclude 'postal' column
        enhanced_rows = enhanced_rows[enhanced_rows['latitude'].notna() & enhanced_rows['longitude'].notna()]
        enhanced_rows = enhanced_rows.drop(['postal'], axis=1)
        # Persist to data warehouse
        records = [list(row) for row in enhanced_rows.itertuples(index=False)] 
        columns = list(enhanced_rows.columns)
        pg_hook.insert_rows(
            table = 'warehouse.int_supermarkets',
            rows = records,
            target_fields = columns,
            commit_every = 500,
            replace=True,
            replace_index="id"
        )
        print("Inserted enhanced data into warehouse.int_supermarkets\n")
        print(pd.DataFrame(records))
    
    # Run tasks
    scrape_supermarkets_ = scrape_supermarkets()
    enhance_pri_price_coords_ = enhance_supermarket_coords(scrape_supermarkets_)
    # Pipeline order
    scrape_supermarkets_ >> enhance_pri_price_coords_ 
