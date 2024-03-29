from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.udacity_plugin import (DataQualityOperator,
                                              LoadDimensionOperator,
                                              LoadFactOperator,
                                              CopyCsvRedshiftOperator,
                                              CopyFixedWidthRedshiftOperator)
from sql_queries import SqlQueries


# Default args for each task of the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2017, 1, 1),
    'end_date': datetime(2017, 12, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


# DAG
dag = DAG('weather_dag',
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

load_zone_table = LoadDimensionOperator(
    task_id='Load_zone_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='zones',
    insert_query=SqlQueries.zone_table_insert,
    delete_load=False
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

load_weather_table = LoadFactOperator(
    task_id='Load_weather_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='weather',
    insert_query=SqlQueries.weather_table_insert
)

load_time_table = LoadDimensionOperator(
    task_id='Load_time_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    insert_query=SqlQueries.time_table_insert,
    delete_load=False
)

quality_checks_zone_table = DataQualityOperator(
    task_id='Quality_checks_zone_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='zone',
    test_query=SqlQueries.zone_table_quality_check,
    expected_res=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set task dependencies
start_operator >> (stage_weather_stations_to_redshift,
                   stage_weather_to_redshift)
stage_weather_stations_to_redshift >> load_weather_stations_table
load_weather_stations_table >> load_zone_table
(stage_weather_to_redshift, load_weather_stations_table) >> load_weather_table
load_weather_table >> load_time_table
(load_zone_table, load_time_table) >> quality_checks_zone_table
quality_checks_zone_table >> end_operator
