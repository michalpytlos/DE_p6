from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.helpers import SqlQueries


# Default args for each task of the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}


# DAG
dag = DAG('dep6_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 1 1 *'
        )


# Tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_table_to_redshift = StageToRedshiftOperator(
    task_id='Stage_table',
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
