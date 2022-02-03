from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/fagcpdebc02_07/Keys/Key_gcs/gcsadminkey.json" # storage_SA
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/fagcpdebc02_07/Keys/Key_bq/bqadminkey.json" # bigquery_SA
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/fagcpdebc02_07/Keys/General_key/key.json"   #default_SA

# Construct a BigQuery client object.
client = bigquery.Client()
# TODO(developer): Set table_id to the ID of the table to create.
table_id = "ssbc-99.mydukan.customer_master"
job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV,
    skip_leading_rows = 1,
    schema = [
            bigquery.SchemaField("customerid", "INTEGER","REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("pincode", "STRING"),          
            ]
    )
uri = "gs://msn-datastudio/customer_master" 
load_job = client.load_table_from_uri(
    uri, table_id, job_config = job_config
)  # Make an API request.
load_job.result()  # Wait for the job to complete.
table = client.get_table(table_id)
print("Loaded {} rows to table {}".format(table.num_rows, table_id))