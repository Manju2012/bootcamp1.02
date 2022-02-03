from faker import Faker
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os

def fake_data_gen(n):
    fake = Faker('en_US')
    schema = avro.schema.parse(open("/home/fagcpdebc02_07/Ingestion/composer/schemafile.avsc", "rb").read())
    writer = DataFileWriter(open("/home/fagcpdebc02_07/Ingestion/composer/samplefile.avro", "wb"), DatumWriter(), schema)
    for _ in range(n):     
        writer.append({"name": fake.name(), 
                        "favorite_number": fake.random.randint(1,100),
                        "favorite_color": fake.random.choice(['pink','purple','red','blue','yellow','white','black'])})
    writer.close()
if __name__ == '__main__':
    n = int(input('Enter the no. of records to be generated : '))
    fake_data_gen(n)
    path= '/home/fagcpdebc02_07/Keys/Key_gcs/gcsadminkey.json'  
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
    client= storage.Client()
    bucket= client.get_bucket('msn-composer')
    blob= bucket.blob('avro_blob')
    blob.upload_from_filename("/home/fagcpdebc02_07/Ingestion/composer/samplefile.avro")
    print(bucket)