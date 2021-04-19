from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta


def login_to_twitter():
    print('login ke twitter')

def input_keyword():
    print('input keyword')

def download_data():
    print('download data')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2021-01-01 13:00:00',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'download_data_twitter',
    default_args = default_args,
    description = 'tarik data dari twitter',
    schedule_interval = timedelta(minutes=5),
    catchup=False
)

task_1 = PythonOperator(
    task_id = "login_to_twitter",
    python_callable = login_to_twitter,
    dag=dag
)

task_2 = PythonOperator(
    task_id = "input_keyword",
    python_callable = input_keyword,
    dag=dag
)

task_3 = PythonOperator(
    task_id = "download_data",
    python_callable = download_data,
    dag=dag
)

task_1 >> task_2 >> task_3