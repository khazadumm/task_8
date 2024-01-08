


import os
import pandas as pd
import csv
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
from airflow import Dataset
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'Igor'
}


SNOWFLAKE_CONN_ID = "snowflake_conn"

with DAG(
    dag_id='deploy_dag',
    template_searchpath = '/home/igor/airflow-snowflake/sql_statesment',
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    start_date=days_ago(1),
    schedule="@once",
    catchup=False,
) as dag:
    snowflake_op_with_params = SnowflakeOperator(
        task_id="deploy_operator",
        sql="deployment.sql"
    )
    
    snowflake_op_with_params
    
    