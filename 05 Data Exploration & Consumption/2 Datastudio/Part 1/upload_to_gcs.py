from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os

path= '/home/fagcpdebc02_07/Keys/Key_gcs/gcsadminkey.json' 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

client= storage.Client()
bucket= client.get_bucket('msn-datastudio')

blob= bucket.blob('customer_master')
blob.upload_from_filename("/home/fagcpdebc02_07/EnC/DSpart1/customer_master.csv")

blob= bucket.blob('order_details')
blob.upload_from_filename("/home/fagcpdebc02_07/EnC/DSpart1/order_details.csv")

blob= bucket.blob('order_quantity')
blob.upload_from_filename("/home/fagcpdebc02_07/EnC/DSpart1/order_quantity.csv")

blob= bucket.blob('product_master')
blob.upload_from_filename("/home/fagcpdebc02_07/EnC/DSpart1/product_master.csv")

print(bucket)