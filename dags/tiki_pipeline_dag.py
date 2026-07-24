from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tiki_daily_pipeline',
    default_args=default_args,
    description='Tiki Data Pipeline: Crawl -> Clean -> Upsert',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Môi trường chạy bên trong Docker nên sẽ gọi qua hostname 'minio' và 'postgres'
    env_vars = {
        **os.environ, 
        'MINIO_URL': 'http://minio:9000',
        'DB_URL': 'jdbc:postgresql://postgres:5432/tiki_db'
    }

    crawl_id = BashOperator(
        task_id='crawl_id',
        bash_command='python /opt/airflow/scripts/crawl_id_local.py',
        env=env_vars
    )

    crawl_data = BashOperator(
        task_id='crawl_data',
        bash_command='python /opt/airflow/scripts/crawl_product_data_local.py',
        env=env_vars
    )

    clean_data = BashOperator(
        task_id='clean_and_upsert_data',
        bash_command='python /opt/airflow/scripts/clean_data_pyspark.py',
        env=env_vars
    )

    # Thứ tự thực thi Pipeline
    crawl_id >> crawl_data >> clean_data
