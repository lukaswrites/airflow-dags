from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator,PythonVirtualenvOperator
import time


#dynamically build dummy dags and tasks

def long_process():
    time.sleep(100)

for i in range(10):
    dag = DAG(
        'dag_id'+i,
        default_args=default_args,
        description='dummy dag',
        schedule_interval=timedelta(minutes=1)
    )
    for j in range(10):
        task = PythonOperator(
            task_id = 'task_id_'+j,
            python_callable=long_process,
            dag=dag
        )