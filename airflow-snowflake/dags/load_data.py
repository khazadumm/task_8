from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG

from airflow.sensors.sql_sensor import SqlSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'Igor'
}


SNOWFLAKE_CONN_ID = "snowflake_conn"

with DAG(
    dag_id='load_data_dag',
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    template_searchpath = '/home/igor/airflow-snowflake/sql_statesment',
    start_date=days_ago(1),
    schedule="@once",
    catchup=False
) as dag:
    
    wait_for_add_file = SqlSensor(
        task_id='wait_for_add_file',
        conn_id=SNOWFLAKE_CONN_ID,
        sql="SELECT EXISTS(SELECT DISTINCT  METADATA$FILENAME FROM @TASK_8.STAGING_SCHEMA.SOURCE_CSV_STAGE T);",
        poke_interval=10,
        timeout=10 * 60
    )

    load_data = SnowflakeOperator(
        task_id="load_data",
        sql="load.sql",
        autocommit=True
    )


    wait_for_add_file >> load_data