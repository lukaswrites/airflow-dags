from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,PythonVirtualenvOperator
import time
from airflow.utils.dates import days_ago
from datetime import timedelta
import random

#dynamically build dummy dags and tasks

def long_process(random_base):
    time.sleep(random_base)
    #file_object = open('/sample.txt', 'a')
    #file_object.write('run')
    #file_object.close()


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


for i in range(1000):
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

    for j in range(60):
        ds_task = PythonOperator(
            task_id = 'task_id_'+str(j),
            python_callable=long_process,
            op_kwargs={'random_base': random.randint(1,100)},
            dag=dag
        )

        if us_task:
            us_task >> ds_task
        
        us_task = ds_task





