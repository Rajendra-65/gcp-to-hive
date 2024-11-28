from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 9, 4),
}

with DAG('list_directories_demo', default_args=default_args, schedule_interval=None) as dag:

    # List contents of /home/airflow/gcs/
    list_gcs_directory = BashOperator(
        task_id='list_gcs_directory',
        bash_command='ls -l /home/airflow/gcs/'
    )

    # List contents of /tmp/
    list_tmp_directory = BashOperator(
        task_id='list_tmp_directory',
        bash_command='ls -l /tmp/'
    )

    list_gcs_directory >> list_tmp_directory
