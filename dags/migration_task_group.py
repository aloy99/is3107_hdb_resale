from airflow.decorators import task_group
from airflow.providers.postgres.operators.postgres import PostgresOperator

@task_group(group_id = 'migration')
def migration():

    create_pg_stg_schema = PostgresOperator(
        task_id = "create_pg_stg_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS staging;"
    )

    create_pg_warehouse_schema = PostgresOperator(
        task_id = "create_pg_warehouse_schema",
        postgres_conn_id = "resale_price_db",
        sql = "CREATE SCHEMA IF NOT EXISTS warehouse;"
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

    create_int_mrts = PostgresOperator(
        task_id = "create_int_mrts",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_mrts.sql"
    )

    create_int_nearest_mrts = PostgresOperator(
        task_id = "create_int_nearest_mrts",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_nearest_mrts.sql"
    )

    create_int_resale_price = PostgresOperator(
        task_id = "create_int_resale_price",
        postgres_conn_id = "resale_price_db",
        sql = "sql/tables/int_resale_prices.sql"
    )

    create_pg_stg_schema >> create_stg_resale_price >> create_stg_mrts
    create_pg_warehouse_schema >> create_int_resale_price
    create_pg_warehouse_schema >> create_int_mrts >> create_int_nearest_mrts 
