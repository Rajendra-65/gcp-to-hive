from datetime import datetime,timedelta

from airflow import DAG

from airflow.providers.apache.hive.operators.hive import HiveOperator

from airflow.operators.bash import BashOperator

from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

date = datetime.now().strftime('%Y%m%d')

fileName = f'/home/airflow/gcs/product_{date}.json'

hql = 'us-central1-airflow-cluster-52566bd2-bucket/dags/product_operation.hql'

raw_file_name = f'product_{date}.json'

gcloud_copy_command = f'gcloud compute scp /home/airflow/gcs/product_{date}.json debasiskhuntia27@cluster-for-spark-practice-m:/home/debasiskhuntia27 --zone=us-east1-c'

clean_up_command = f'rm /home/airflow/gcs/product_{date}.json'

objectName = f'products/product_{date}.json'

clear_from_cluster_command = f"gcloud compute ssh debasiskhuntia27@cluster-for-spark-practice-m --command='rm /home/debasiskhuntia27/{raw_file_name}.json'"

default_args = {
    'owner':'airflow',
    'depends_on_past':True,
    'email_on_failure':False,
    'email_on_success':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    'gcp-bucket-to-hive-table',
    default_args = default_args,
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2024,9,3),
    catchup = False,
    tags = ['dev']
)

downloading_the_file_from_bucket = GCSToLocalFilesystemOperator(
    task_id = 'downloading_the_file_from_bucket',
    object_name = objectName,
    bucket = 'airflow_assignment_2',
    filename = fileName,
    dag=dag
)

copy_the_file_to_the_dataproc_cluster = BashOperator(
    task_id = 'copy_the_file_to_the_dataproc_cluster',
    bash_command = gcloud_copy_command,
    depends_on_past=True,
    dag = dag
)

clean_up_local_file = BashOperator(
    task_id="clean_up_local_file",
    bash_command=clean_up_command,
    depends_on_past=True,
    dag  = dag
)

download_hql_file = GCSToLocalFilesystemOperator(
    task_id='download_hql_file',
    bucket='us-central1-airflow-cluster-52566bd2-bucket',
    object_name='dags/product_operation.hql',
    filename='/home/airflow/gcs/product_operation.hql',
    depends_on_past=True,
    dag=dag
)


updating_value_in_hive = HiveOperator(
    task_id = 'updating_value_in_hive',
    hql ='/home/airflow/gcs/product_operation.hql',
    hive_cli_conn_id='hive_cli_default', 
    hiveconfs={"date": "{{ ds_nodash }}"},
    schema='default',
    depends_on_past=True,
    dag = dag
)

delete_file_from_cluster = BashOperator(
    task_id="delete_file_from_cluster",
    bash_command=clear_from_cluster_command,
    depends_on_past=True,
    trigger_rule='all_done',
    dag = dag
)

downloading_the_file_from_bucket >> copy_the_file_to_the_dataproc_cluster >> clean_up_local_file >>download_hql_file >> updating_value_in_hive >> delete_file_from_cluster