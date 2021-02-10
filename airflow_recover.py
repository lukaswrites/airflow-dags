from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

import pandas as pd

import subprocess

import datetime

class Dag:
    def __init__(self,id):
        self.id = id
        self.task_instances = []

db_creds['host'] = 'database-1.ch6lc1dskajd.us-east-1.rds.amazonaws.com'
db_creds['user'] = 'cloud_user'
db_creds['password'] = 'cybersoft'
db_creds['port'] = 5432


url = URL(username=db_creds['user'], host=db_creds['host'], port=db_creds['port'],
                password=db_creds['password'], drivername='postgres', database='cloud_user')
engine = create_engine(url)
conn = engine.connect()

def get_next_execution_date(dag):
    res = subprocess.run(["airflow","next_execution",dag.id],capture_output=True)
    next_execution_date_str = str(res.stdout).split("\\n")[-2]

    return next_execution_date_str

def pause_dag(dag):
    logger.info(f"Pausing dag: {dag.id}")
    res = subprocess.run(["airflow","pause",dag.id],capture_output=True)
    if res.returncode > 0:
        raise Exception(f"failed to pause dag {dag.id}")
    else:
        logger.info(f"Dag {dag.id} paused")

def resume_dag(dag):
    logger.info(f"Resume dag: {dag.id}")
    res = subprocess.run(["airflow","unpause",dag.id],capture_output=True)
    if res.returncode > 0:
        raise Exception(f"failed to resume dag {dag.id}")
    else:
        logger.info(f"Dag {dag.id} resumed")

def get_active_dags():
    #get active dags and their last successfull execution date
    q_active_dags = "select distinct dag_id as id from dag where is_active=true and is_paused=false"
    df = pd.read_sql(q_active_dags,con=conn)

    dags_list = []

    df.apply(lambda row: dags_list.append(Dag(row['id'])))

    logger.info(f"Found {len(dags_list)} active dags")

    for dag in dags_list:
        logger.info(f"Getting task instances for {dag.id}")
        q_task_instances = f"select distinct task_id from task_instance where dag_id = {dag.id}"
        task_instances = list(pd.read_sql(q_task_instances,con=conn))
        dag.task_instances = task_instances
        logger.info(f"Found {len(task_instances)} task instances for {dag.id}")

    return dags_list

def execute_task():
    pass

def create_new_job(dag,execution_date,start_date,end_date):
    logger.info(f"Creating job id for dag {dag.id}")

    q_insert_job = f"""
        insert into job (dag_id,state,job_type,start_date,end_date,latest_heartbeat,executor_class,hostname,unixname)
        values ({dag.id},'success','LocalTaskJob',{start_date},{end_date},{end_date},'NoneType','FixerHost','ubuntu')
    """
    statement = text(q_insert_job)

    conn.execute(statement)

    q_job_id = "select max(id) from job"

    rs = conn.execute(text(q_job_id))

    job_id = rs[0][0]
    logger.info(f"Job id {job_id} created")

    return job_id


def create_new_task_instances(dag,job_id,execution_date):
    logger.info(f"Creating task instances for Dag {dag.id} with job_id {job_id}")

    ti_start_date = execution_date
    ti_end_date = ti_start_date + datetime.timedelta(minutes=1)

    for ti in dag.task_instances:
        logger.info(f"Creating task instance: {ti}")
        q_insert_tis = f"""
            insert into task_instance(task_id,dag_id,execution_date,start_date,end_date,duration,state,try_number,hostname,unixname,job_id,pool,queue,priority_weight,queued_dttm,pid,max_tries,executor_config,pool_slots)
            values ({ti},{dag.id},{execution_date},{ti_start_date},{ti_end_date},60,'success',1,'FixerHost','ubuntu',{job_id},'default_pool','airflow',1,{ti_start_date},12332,1,null,1)
        """

        statement = text(q_insert_tis)

        conn.execute(statement)

        logger.info(f"Task instance {ti} created")


def create_new_dag_runs(dag,to_execution_date):

    while (dag.last_exec_date < to_execution_date):
        logger.info(f"Creating dag run for {dag.id}")
        
        next_execution_date_str = get_next_execution_date(dag)
        next_execution_date = datetime.datetime.strptime(next_execution_date_str,"%Y-%m-%d %H:%M:%S%z")
        run_id = 'scheduled__'+next_execution_date_str.replace(' ','T')

        start_date = next_execution_date_str + datetime.timedelta(minutes=1)
        end_date = start_date + datetime.timedelta(minutes=1)

        #actually do the task
        execute_task()

        #create new dag run in the db
        q_insert_dag_run = f"""
            insert into dag_run (dag_id,execution_date,state,run_id,external_trigger,conf,end_date,start_date)
            values ({dag.id},{next_execution},'success',{run_id},false,null,{end_date},{start_date})
        """

        statement = text(q_insert_dag_run)

        conn.execute(statement)

        logger.info(f"Dag run with execution_date {next_execution_date} created")

        job_id = create_new_job(dag,next_execution_date,start_date,end_date)
        create_new_task_instances(dag,next_execution_date,job_id)

    
    logger.info(f"Processing Dag {dag.id} has been completed")

def backfill_dag(dag,to_execution_date):
    pause_dag(dag)
    create_new_dag_runs(dag,to_execution_date)
    resume(dag)
    

def recover_airflow(hour):
    #:param hour: up to how many hour before present hour
    active_dags = get_active_dags()

    #get nearest hour back from present hour
    to_execution_date = datetime.datetime.now().replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=hour)

    for dag in active_dags:
        backfill_dag(dag,to_execution_date)


if __name__=='__main__':
    recover_airflow(1)







