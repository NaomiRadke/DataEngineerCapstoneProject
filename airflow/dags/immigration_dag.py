from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator

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
          schedule_interval='0 0 1 * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="Create_Tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

# stage event to redshift for all tables
immigration_to_redshift = StageToRedshiftOperator(
    task_id="stage_immigration_fact_table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.immigration",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/immigration.parquet",
    region="eu-central-1",
    dag=dag
)

demographics_to_redshift = StageToRedshiftOperator(
    task_id="stage_demographics_dimension_table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.demographics",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/demographics.parquet",
    region="eu-central-1",
    dag=dag
)

countries_to_redshift = StageToRedshiftOperator(
    task_id="stage_countries_dimension_table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.countries",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/countries.parquet",
    region="eu-central-1",
    dag=dag
)

date_to_redshift = StageToRedshiftOperator(
    task_id="stage_arrival_dates_dimension_table",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.arrivalDate",
    s3_bucket="udacity-data-engineer",
    s3_prefix="tables/arrivaldates.parquet",
    region="eu-central-1",
    dag=dag
)

# run quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['immigration', 'demographics', 'countries', 'arrivaldates']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#
# Task ordering for the DAG tasks
#
start_operator >> create_tables
create_tables >> [demographics_to_redshift, countries_to_redshift, date_to_redshift]
[demographics_to_redshift, countries_to_redshift, date_to_redshift] >> run_quality_checks
run_quality_checks >> end_operator
