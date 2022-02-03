from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/fagcpdebc02_07/Keys/Key_gcs/gcsadminkey.json" # storage_SA
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/fagcpdebc02_07/Keys/Key_bq/bqadminkey.json" # bigquery_SA
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/fagcpdebc02_07/Keys/General_key/key.json"   #default_SA

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "ssbc-99.mydukan.order_quantity"
job_config = bigquery.LoadJobConfig(
    source_format = bigquery.SourceFormat.CSV,
    skip_leading_rows = 1,
    schema = [
            bigquery.SchemaField("orderid", "INTEGER","REQUIRED"),
            bigquery.SchemaField("productid", "INTEGER","REQUIRED"),
            bigquery.SchemaField("quantity", "INTEGER"),
            ]
    )
uri = "gs://msn-datastudio/order_quantity" 
load_job = client.load_table_from_uri(
    uri, table_id, job_config = job_config
)  # Make an API request.
load_job.result()  # Wait for the job to complete.
table = client.get_table(table_id)
print("Loaded {} rows to table {}".format(table.num_rows, table_id))