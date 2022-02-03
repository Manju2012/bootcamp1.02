from google.cloud import bigquery
import os

bq_path = "/home/fagcpdebc02_07/Keys/Key_bq/bqadminkey.json"  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= bq_path

# Construct a BigQuery client object
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "ssbc-99.pgdataset.dept_summary"
schema = [    
    bigquery.SchemaField("dept_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("dept_name", "STRING"),
    bigquery.SchemaField("emp_count", "INT64"),
    bigquery.SchemaField("total_cost", "INT64"),
    bigquery.SchemaField("proj_count", "INT64")
    ]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  
# Make an API request
print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
job_config = bigquery.QueryJobConfig(destination=table_id)
sql1 = """
    SELECT dept.dept_id,dept.dept_name,count(projs.emp_id) as emp_count,sum(emp.salary) as total_cost, count(distinct(projs.proj_id)) as proj_count
    FROM `ssbc-99.pgdataset.department` dept
    INNER JOIN 
    `ssbc-99.pgdataset.employee` emp
    ON dept.dept_id=emp.dept_id
    INNER JOIN 
    `ssbc-99.pgdataset.project_staff` projs
    ON emp.emp_id=projs.emp_id
    WHERE emp.is_active = TRUE
    group by dept_id,dept_name;
"""
# Start the query, passing in the extra configuration.
query_job = client.query(sql1, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.
print("Query results loaded to the table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
results = query_job.result()  # Waits for job to complete.


table_id = "ssbc-99.pgdataset.project_staff_count"
schema = [    
    bigquery.SchemaField("proj_id", "INT64", mode="REQUIRED"),    
    bigquery.SchemaField("staff_count", "INT64")
    ]
table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)  
# Make an API request
print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
job_config = bigquery.QueryJobConfig(destination=table_id)
sql2 = """
    SELECT proj_id,count(emp_id) as staff_count
    FROM `ssbc-99.pgdataset.project_staff`
    group by proj_id;
"""
# Start the query, passing in the extra configuration.
query_job = client.query(sql2, job_config=job_config)  # Make an API request.
query_job.result()  # Wait for the job to complete.
print("Query results loaded to the table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
results = query_job.result()  # Waits for job to complete.