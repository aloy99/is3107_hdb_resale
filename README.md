## Docker Environment Setup
1. `docker build . -f Dockerfile --pull --tag is3107-airflow:0.0.1`
2. `docker compose up airflow-init`
3. `docker compose up`

## Docker Containers
To exec into docker container
```docker exec -it <<container_id>> bash```

To extract pdfs
```docker cp <<container_id>>:/opt/airflow/<<report_name>>.pdf .```

## Using Postgres
To exec into postgres docker container
```docker exec -it <<container_id>> psql -U user -d resale_price_proj```

To select and view all tables
```SET search_path TO staging, public;```
```\dt```