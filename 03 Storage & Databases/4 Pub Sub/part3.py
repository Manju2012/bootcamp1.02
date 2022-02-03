from google.cloud import pubsub_v1
import os
from google.cloud import bigquery

publisher = pubsub_v1.PublisherClient()

path= "/home/fagcpdebc02_07/Keys/Key_pubsub/pubsubadminkey.json" # provide path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=path

# TODO(developer)
project_id= "ssbc-99"
topic_id= "topic1"
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

msg=["xxx","yyy","ccc","zzz","bbb"]

for n in range(6, 11):
    data = "{id:" + str(n) + ", name:" + msg[n-6] + "}"
    # Data must be a bytestring
    data = data.encode("utf-8")
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, data)
    print(future.result())

print("Published messages to " + topic_path)