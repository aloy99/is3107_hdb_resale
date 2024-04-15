from airflow.decorators import task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator


@task_group(group_id = "migration")
def migration_tasks():

    create_pg_stg_schema = PostgresOperator(
        task_id = "create_pg_stg_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS staging;"
    )

    create_stg_resale_price = PostgresOperator(
        task_id = "create_stg_resale_price",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_resale_prices.sql"
    )

    create_stg_mrts = PostgresOperator(
        task_id = "create_stg_mrts",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_mrts.sql"
    )  

    create_stg_parks = PostgresOperator(
        task_id = "create_stg_parks",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_parks.sql"
    )

    create_stg_pri_schools = PostgresOperator(
        task_id = "create_stg_pri_schools",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/stg_pri_schools.sql"
    )

    create_pg_warehouse_schema = PostgresOperator(
        task_id = "create_pg_warehouse_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS warehouse;"
    )

    create_int_resale_price = PostgresOperator(
        task_id = "create_int_resale_price",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_resale_prices.sql"
    )

    create_int_mrts = PostgresOperator(
        task_id = "create_int_mrts",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_mrts.sql"
    ) 

    create_int_nearest_mrt = PostgresOperator(
        task_id = "create_int_nearest_mrt",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_nearest_mrt.sql"
    )

    create_int_parks = PostgresOperator(
        task_id = "create_int_parks",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_parks.sql"
    )

    create_int_nearest_park = PostgresOperator(
        task_id = "create_int_nearest_park",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_nearest_park.sql"
    )

    create_int_pri_schools = PostgresOperator(
        task_id = "create_int_pri_schools",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_pri_schools.sql"
    )

    create_int_nearest_pri_schoolss = PostgresOperator(
        task_id = "create_int_nearest_pri_schoolss",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_nearest_pri_schools.sql"
    )

    create_pg_stg_schema >> [create_stg_resale_price, create_stg_mrts, create_stg_parks, create_stg_pri_schools]
    create_pg_warehouse_schema >> [create_int_mrts, create_int_nearest_mrt, create_int_resale_price, create_int_parks, create_int_nearest_park, create_int_pri_schools, create_int_nearest_pri_schoolss] 
