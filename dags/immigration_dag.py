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
immigration_to_redshift = StageToRedshiftOperator(
    task_id="stage immigration fact table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.immigration",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/immigration.parquet",
    options="PARQUET",
    region="eu-central-1",
    dag=dag
)

demographics_to_redshift = StageToRedshiftOperator(
    task_id="stage demographics dimension table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.demographics",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/demographics.parquet",
    options="PARQUET",
    region="eu-central-1",
    dag=dag
)

countries_to_redshift = StageToRedshiftOperator(
    task_id="stage countries dimension table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.countries",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/countries.parquet",
    options="PARQUET",
    region="eu-central-1",
    dag=dag
)

date_to_redshift = StageToRedshiftOperator(
    task_id="stage arrival dates dimension table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.arrivalDate",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/arrivalDates.parquet",
    options="PARQUET",
    region="eu-central-1",
    dag=dag
)

# run quality checks

end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)