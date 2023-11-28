from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False,
}

dag = DAG('immigration-data-dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = EmptyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="Create_Tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

# stage event to redshift for all tables

# run quality checks
