from google.cloud import storage
from datetime import datetime
from google.cloud import bigquery
import opentelemetry
import json


def gcs_create(event, context):
    # Printing initial input
    file = event
    print(f"***** The file {file['name']} was created in {file['bucket']} at {datetime.now().date()}T{datetime.now().time()}Z")

    # Starting with copy funtion
    client = storage.Client()
    source_bucket = client.get_bucket("abdullah-hw4-bucket")
    destination_bucket = client.get_bucket("abdullah-hw4-bucket-backup")
    
    files = source_bucket.list_blobs()

    for blob in files:
        if blob.name == file['name']:
            source_bucket.copy_blob(blob, destination_bucket)
            print(f"***** The file {blob.name} was copied to {destination_bucket.name}")

def bq_load(event, context):
    # Printing initial input
    file = event
    print(f"***** The file {file['name']} was created in {file['bucket']} at {datetime.now().date()}T{datetime.now().time()}Z")

    #Creating clients
    gcs_client = storage.Client()
    bq_client = bigquery.Client()

    #Downloading file as string
    bucket = gcs_client.get_bucket("abdul-data228-hw5")
    blob = bucket.get_blob(file['name'])
    json_str = blob.download_as_string()
    json_dict = json.loads(json_str)

    #Setting up relevant table in Big Query
    table_id = "data228.data228_hw5.activity"

    #Inserting json data to appropriate table
    errors = bq_client.insert_rows_json(table_id, [json_dict])
    assert errors == []
    
    print(f"***** JSON data {json_str} into {table_id} table")



