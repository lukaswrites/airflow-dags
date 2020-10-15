from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,PythonVirtualenvOperator
import time
from airflow.utils.dates import days_ago
from datetime import timedelta
import random

#dynamically build dummy dags and tasks

def long_process():
    for i in range(100):
        time.sleep(random.uniform(1,10))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag_bag = []


for i in range(20):
    dag_id = 'dag_id'+str(i)
    dag = DAG(
        'dag_id'+str(i),
        default_args=default_args,
        description='dummy dag',
        schedule_interval=timedelta(minutes=5)
    )

    globals()[dag_id] = dag
    
    for j in range(10):
        task = PythonOperator(
            task_id = 'task_id_'+str(j),
            python_callable=long_process,
            dag=dag
        )

