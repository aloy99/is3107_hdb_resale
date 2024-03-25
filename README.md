## Docker Environment Setup
1. `docker build . -f Dockerfile --pull --tag is3107-airflow:0.0.1`
2. `docker compose up airflow-init`
3. `docker compose up`

To exec into a docker container
```docker exec -u airflow -it <<container_id>> bash```

To extract pdfs
```docker cp <<container_id>>:/opt/airflow/<<report_name>>.pdf .```