from google.cloud import bigquery

def load_csv_to_bq(data, context):
        gcs_uri = 'gs://' + data['bucket'] + '/' + data['name']
        table_id = 'ssbc-99.cfunc.table_cfunc'
        #table_id = "{}:{}.{}".format(project_id, bq_dataset, bq_table)
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField("Name", "STRING"),
                        bigquery.SchemaField("Fav_number", "INT64"),
                        bigquery.SchemaField("Fav_colour", "STRING")],
                skip_leading_rows=1, source_format=bigquery.SourceFormat.CSV)
        load_job = client.load_table_from_uri(
                gcs_uri, table_id, job_config=job_config
        )
        load_job.result()
        print('Job finished')