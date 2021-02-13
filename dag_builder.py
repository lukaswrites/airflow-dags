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

    for c_df in pd.read_csv('s3://kuhontol3/test.csv.gz', compression='gzip', header=0, chunksize=c_size,nrows=10000):
        df_exploded = c_df['records'].apply(pd.Series)


def long_process(random_base):
    transform_task(sleep_time=random_base)
    #time.sleep(random_base)
    #file_object = open('/sample.txt', 'a')
    #file_object.write('run')
    #file_object.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2021-02-10 13:00:00',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag_bag = []


for i in range(64):
    dag_id = 'dag_id'+str(i)
    dag = DAG(
        'dag_id'+str(i),
        default_args=default_args,
        description='dummy dag',
        schedule_interval=timedelta(minutes=5),
        catchup=True
    )

    globals()[dag_id] = dag
    
    us_task = None

    for j in range(10):
        ds_task = PythonOperator(
            task_id = 'task_id_'+str(j),
            python_callable=long_process,
            op_kwargs={'random_base': random.randint(1,10)},
            dag=dag
        )

        if us_task:
            us_task >> ds_task
        
        us_task = ds_task





