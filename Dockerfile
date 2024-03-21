FROM apache/airflow:2.7.3
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
RUN pip install -r requirements.txt