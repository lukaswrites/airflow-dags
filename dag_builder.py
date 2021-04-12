from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,PythonVirtualenvOperator
import time
from airflow.utils.dates import days_ago
from datetime import timedelta
import random
import pandas as pd

#dynamically build dummy dags and tasks

def transform_task(sleep_time):
    c_size = 200000

    for c_df in pd.read_csv('s3://kuhontol/test.csv.gz', compression='gzip', header=0, chunksize=c_size,nrows=10000):
        df_exploded = c_df['records'].apply(pd.Series)


def long_process(random_base):
    #transform_task(sleep_time=random_base)
    time.sleep(random_base)
    #file_object = open('/sample.txt', 'a')
    #file_object.write('run')
    #file_object.close()


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
    'my_first_dag',
    default_args=default_args,
    description='dummy dag',
    schedule_interval=timedelta(minutes=5),
    catchup=True
)

ds_task = PythonOperator(
    task_id = 'task_id_1',
    python_callable=long_process,
    op_kwargs={'random_base': random.randint(1,10)},
    dag=dag
        )

ds_task_2 = PythonOperator(
    task_id = 'task_id_2',
    python_callable=long_process,
    op_kwargs={'random_base': random.randint(1,10)},
    dag=dag
        )

ds_task >> ds_task_2





