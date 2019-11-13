from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (LoadFactOperator, LoadDimensionOperator,
                               DataQualityOperator)

from airflow.operators.udacity_plugin import ETAirQualityOperator
# from airflow.helpers.udacity_plugin import SqlQueries


# Default args for each task of the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12)
}


# DAG
dag = DAG('airquality_dag',
          default_args=default_args,
          description='ETL for air quality data',
          schedule_interval='0 0 1 1 *'
          )

# Tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

et_airquality = ETAirQualityOperator(
    task_id='et_airquality',
    aws_key_id=Variable.get('aws_access_key_id'),
    aws_secret_key=Variable.get('aws_secret_access_key'),
    s3_in_bucket=Variable.get('s3_in_aq_bucket'),
    s3_in_prefix=Variable.get('s3_in_aq_prefix'),
    s3_out_bucket=Variable.get('s3_out_aq_bucket'),
    s3_out_prefix=Variable.get('s3_out_aq_prefix'),
    dag=dag
)

load_fact_table = LoadFactOperator(
    task_id='Load_fact_table',
    dag=dag
)

load_dimension_table = LoadDimensionOperator(
    task_id='Load_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
