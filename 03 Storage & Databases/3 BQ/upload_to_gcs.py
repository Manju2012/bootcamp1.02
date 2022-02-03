from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os
path= '/home/fagcpdebc02_07/Keys/Key_gcs/gcsadminkey.json'  
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
client= storage.Client()
bucket= client.get_bucket('msn-netflix')
blob= bucket.blob('netflix_blob')
blob.upload_from_filename("/home/fagcpdebc02_07/SnD/bq-tasks/bq4_netflix/netflix-rotten-tomatoes-metacritic-imdb.csv")
print(bucket)
