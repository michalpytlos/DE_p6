from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.udacity_plugin import (DataQualityOperator,
                                              LoadDimensionOperator,
                                              CopyCsvRedshiftOperator,
                                              CopyFixedWidthRedshiftOperator)
from sql_queries import SqlQueries


# Default args for each task of the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2013, 3, 12),
    'end_date': datetime(2014, 3, 12)
}


# DAG
dag = DAG('weather_dag3',
          default_args=default_args,
          description='ETL for weather data',
          schedule_interval='0 0 1 1 *'
          )

# Tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_weather_stations_to_redshift = CopyFixedWidthRedshiftOperator(
    task_id='Stage_stations_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_weather_stations',
    s3_bucket=Variable.get('s3_weather_bucket'),
    s3_key=Variable.get('s3_weather_stations_key'),
    arn=Variable.get('iam_role_arn'),
    fixedwidth_spec=Variable.get('s3_weather_stations_spec'),
    maxerror=Variable.get('s3_weather_stations_maxerror'),
    load_delete=True
)

load_weather_stations_table = LoadDimensionOperator(
    task_id='Load_weather_stations_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='weather_stations',
    insert_query=SqlQueries.weather_stations_table_insert,
    delete_load=True
)

stage_weather_to_redshift = CopyCsvRedshiftOperator(
    task_id='Stage_weather',
    dag=dag,
    redshift_conn_id='redshift',
    table='staging_weather',
    s3_bucket=Variable.get('s3_weather_bucket'),
    s3_prefix=Variable.get('s3_weather_prefix'),
    data_type='weather',
    arn=Variable.get('iam_role_arn'),
    ignore_header=0,
    compression='gzip'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set task dependencies
stage_weather_stations_to_redshift >> load_weather_stations_table
