from airflow.decorators import task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scraper.datagov.park_scraper import ParkScraper

@task_group(group_id = "parks")
def park_tasks():
    @task
    def scrape_parks():
        park_scraper = ParkScraper({})
        pg_hook = PostgresHook("resale_price_db")
        park_rows = park_scraper.get_parks()
        for park in park_rows:
            pg_hook.run("""
                INSERT INTO staging.stg_parks (park, latitude, longitude)
                VALUES (%s, %s, %s) ON CONFLICT (park) DO NOTHING;
            """, parameters=(park['park'], park['latitude'], park['longitude']))

    # Run tasks
    scrape_parks_ = scrape_parks()
    # Pipeline order
    scrape_parks_
