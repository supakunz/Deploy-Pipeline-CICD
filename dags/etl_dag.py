from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

import sys
sys.path.append("/opt/airflow/plugins")

from load_data import load_data
from clean_date import run_pipeline
from handle_missing_value import handle_missing_value
from load_to_warehouse import load_to_warehouse
from validate_data import validate_data

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Sample ETL pipeline with Pandas',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'pandas'],
)

start = PythonOperator(
    task_id='start',
    python_callable=lambda: print("ETL process started."),
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'output_path': '/opt/airflow/data/raw/covid_19_raw.csv'},
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=run_pipeline,
    op_kwargs={
        'input_path': '/opt/airflow/data/raw/covid_19_raw.csv',
        'output_path': '/opt/airflow/data/output/covid_19_clean.csv'
    },
    dag=dag,
)

handle_missing_value_task = PythonOperator(
    task_id='handle_missing_value',
    python_callable=handle_missing_value,
    op_kwargs={
        'input_path': '/opt/airflow/data/output/covid_19_clean.csv',
        'output_path': '/opt/airflow/data/output/covid_19_clean.csv'
    },
    dag=dag,
)

# validate_data_task = GreatExpectationsOperator(
#     task_id='validate_data_ge',
#     data_context_root_dir="/opt/airflow/include/gx", # ที่อยู่ของ Great Expectations context
#     checkpoint_name="covid_checkpoint", # ชื่อ checkpoint ที่สร้างใน Great Expectations
#     fail_task_on_validation_failure=True,
#     do_xcom_push=False,
#     dag=dag,
# )

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    op_kwargs={
        'path_to_file': '/opt/airflow/data/output/covid_19_clean.csv',
        'ge_context_path': '/opt/airflow/include/gx'
        },
    do_xcom_push=False,
    dag=dag,
)

load_to_warehouse_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    op_kwargs={'input_path': '/opt/airflow/data/output/covid_19_clean.csv',
               'output_path': '/opt/airflow/data/output/covid_19_clean.csv'},
    dag=dag,
)

end = PythonOperator(
    task_id='end',
    python_callable=lambda: print("ETL process completed."),
    dag=dag,
)

# Set task dependencies
start >> load_data_task >> clean_data_task >> handle_missing_value_task >> validate_data_task >> load_to_warehouse_task >> end