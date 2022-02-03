import psycopg2
import datetime
import os
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import pytz
import logging
import google.cloud.logging
import faulthandler

faulthandler.enable()

bq_path = "/home/fagcpdebc02_07/Keys/Key_bq/bqadminkey.json"  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= bq_path

def query(q):
    conn = psycopg2.connect(host="35.225.252.141", database="myorg", user="postgres", password="test@123")
    return pd.read_sql(q,conn)

project = 'ssbc-99'
client = bigquery.Client(project=project)
job_config = bigquery.LoadJobConfig()
job_config.autodetect = True
dataset= 'pgdataset'

table=['project','employee','project_staff','department']

schema = [
        [
            {'name':'proj_id', 'type': 'INT64'},
            {'name':'proj_name', 'type': 'STRING'},
            {'name':'dept_id', 'type': 'INT64'},
            {'name':'proj_start_date', 'type': 'DATE'},
            {'name':'proj_end_date', 'type': 'DATE'}
        ],
        [
            {'name':'emp_id', 'type':'INT64'},
            {'name':'proj_name', 'type': 'STRING'},
            {'name':'name', 'type':'STRING'},
            {'name':'dept_id', 'type':'INT64'},
            {'name':'salary', 'type':'INT64'},
            {'name':'joining_date', 'type':'DATE'},
            {'name':'leaving_date', 'type':'DATE'},
            {'name':'is_active', 'type':'BOOLEAN'}
        ],
        [
            {'name':'proj_id', 'type': 'INT64'},
            {'name':'emp_id', 'type': 'INT64'},
            {'name':'role_name', 'type': 'STRING'},
            {'name':'start_date', 'type': 'DATE'},
            {'name':'end_date', 'type': 'DATE'}
        ],
        [
            {'name':'dept_id', 'type': 'INT64'},
            {'name':'dept_name', 'type': 'STRING'},
            {'name':'dept_head_id', 'type': 'INT64'}
        ]
    ]

st='SELECT * FROM ' + 'project'
df = query(st)
table_id="ssbc-99.{}.{}".format(dataset,'project')
table = bigquery.Table(table_id)
job1 = pandas_gbq.to_gbq(df, table_id, table_schema=schema[0], if_exists="replace")
print("Loaded {}".format( table_id ))

st='SELECT * FROM ' + 'employee'
df = query(st)
table_id="ssbc-99.{}.{}".format(dataset,'employee')
table = bigquery.Table(table_id)
job2 = pandas_gbq.to_gbq(df, table_id, table_schema=schema[1], if_exists="replace")
print("Loaded {}".format( table_id ))

st='SELECT * FROM ' + 'project_staff'
df = query(st)
table_id="ssbc-99.{}.{}".format(dataset,'project_staff')
table = bigquery.Table(table_id)
job3 = pandas_gbq.to_gbq(df, table_id, table_schema=schema[2], if_exists="replace")
print("Loaded {}".format( table_id ))

st='SELECT * FROM ' + 'department'
df = query(st)
table_id="ssbc-99.{}.{}".format(dataset,'department')
table = bigquery.Table(table_id)
job4 = pandas_gbq.to_gbq(df, table_id, table_schema=schema[3], if_exists="replace")
print("Loaded {}".format( table_id ))
