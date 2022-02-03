import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


default_args = {
    'owner': 'Samiksha',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['Your email address'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG('env1-dag',
        schedule_interval='@once',
        default_args=default_args
        )

BQ_CONN_ID = 'bigquery_default'
BQ_PROJECT = 'ssbc-99'
BQ_DATASET = 'ssbc-99:dag_dataset'

t1 = GoogleCloudStorageToBigQueryOperator(
task_id='env1-dag',
bucket= 'msn-composer',
source_objects = ['avro_blob'],
schema_fields=[
     {'name': 'name', 'type': 'STRING'},
     {'name': 'favorite_number',  'type': 'INT64', 'mode': 'NULLABLE'},
     {'name': 'favorite_color', 'type': 'STRING', 'mode': 'NULLABLE'}
 ],
destination_project_dataset_table='ssbc-99.dag_dataset.composer_data',
write_disposition='WRITE_TRUNCATE',
source_format='AVRO',
google_cloud_storage_conn_id='google_cloud_storage_default',
bigquery_conn_id=BQ_CONN_ID,
dag = dag
)


t1