from datetime import timedelta
from email.policy import default
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from pipeline import downloader, load_raw_to_s3, transform_data, load_to_dw


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 10, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'cvm_dag',
    default_args=default_args,
    description='cvm funds etl'
)

extract = PythonOperator(
    task_id='extract_from_source',
    python_callable=downloader,
    dag=dag
)

load_raw = PythonOperator(
    task_id='load_raw',
    python_callable=load_raw_to_s3,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_dw = PythonOperator(
    task_id='load_dw',
    python_callable=load_to_dw,
    dag=dag
)


extract >> load_raw >> transform >> load_dw