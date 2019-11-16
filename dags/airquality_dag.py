from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.udacity_plugin import (ETAirQualityOperator,
                                              CopyCsvRedshiftOperator,
                                              DataQualityOperator)
from sql_queries import SqlQueries


# Default args for each task of the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2017, 1, 1),
    'end_date': datetime(2017, 1, 3),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


# DAG
dag = DAG('airquality_dag',
          default_args=default_args,
          description='ETL for air quality data',
          schedule_interval='0 0 * * *'
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
    data_type='air_quality',
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

load_attribution_table = CopyCsvRedshiftOperator(
    task_id='Load_attribution_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='attributions',
    s3_bucket=Variable.get('s3_out_aq_bucket'),
    s3_prefix=Variable.get('s3_out_aq_prefix'),
    data_type='air_quality',
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
    data_type='air_quality',
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
    data_type='air_quality',
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
    data_type='air_quality',
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
    data_type='air_quality',
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
    data_type='air_quality',
    arn=Variable.get('iam_role_arn'),
    ignore_header=1
)

quality_checks_zone_table = DataQualityOperator(
    task_id='Quality_checks_zone_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='zone',
    test_query=SqlQueries.zone_table_quality_check,
    expected_res=0
)

quality_checks_air_quality_table = DataQualityOperator(
    task_id='Quality_checks_air_quality_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='air_quality',
    test_query=SqlQueries.air_quality_table_quality_check,
    expected_res=0
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
load_air_quality_table >> (quality_checks_zone_table,
                           quality_checks_air_quality_table) >> end_operator
