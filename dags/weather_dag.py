from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import DataQualityOperator
from airflow.operators.udacity_plugin import CopyFixedWidthRedshiftOperator
# from airflow.helpers.udacity_plugin import SqlQueries


# Default args for each task of the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2013, 3, 12),
    'end_date': datetime(2014, 3, 12)
}


# DAG
dag = DAG('weather_dag1',
          default_args=default_args,
          description='ETL for weather data',
          schedule_interval='0 0 1 1 *'
          )

# Tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

load_stations_table = CopyFixedWidthRedshiftOperator(
    task_id='Load_stations_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='weather_stations',
    s3_bucket=Variable.get('s3_weather_bucket'),
    s3_key=Variable.get('s3_weather_stations_key'),
    arn=Variable.get('iam_role_arn'),
    fixedwidth_spec=Variable.get('s3_weather_stations_spec'),
    maxerror=Variable.get('s3_weather_stations_maxerror')
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set task dependencies
