from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import DataQualityOperator
from airflow.operators.udacity_plugin import ETAirQualityOperator, CopyCsvRedshiftOperator
# from airflow.helpers.udacity_plugin import SqlQueries


# Default args for each task of the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2013, 3, 12),
    'end_date': datetime(2014, 3, 12)
}


# DAG
dag = DAG('airquality1_dag',
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

load_air_quality_table = CopyCsvRedshiftOperator(
    task_id='Load_air_quality_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='air_quality',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

load_attribution_table = CopyCsvRedshiftOperator(
    task_id='Load_attribution_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='air_quality',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

load_city_table = CopyCsvRedshiftOperator(
    task_id='Load_city_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='cities',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

load_location_table = CopyCsvRedshiftOperator(
    task_id='Load_location_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='locations',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

load_source_table = CopyCsvRedshiftOperator(
    task_id='Load_source_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='sources',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

load_time_table = CopyCsvRedshiftOperator(
    task_id='Load_time_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

load_zone_table = CopyCsvRedshiftOperator(
    task_id='Load_zone_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='zones',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set task dependencies
start_operator >> et_airquality
et_airquality >> (load_attribution_table,
                  load_source_table,
                  load_time_table,
                  load_zone_table)
load_zone_table >> (load_city_table, load_location_table)
(load_attribution_table,
 load_source_table,
 load_time_table,
 load_city_table,
 load_location_table) >> load_air_quality_table
load_air_quality_table >> run_quality_checks
run_quality_checks >> end_operator
