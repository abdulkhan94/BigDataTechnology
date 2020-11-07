from google.cloud import storage
client = storage.Client()

bucket = client.get_bucket("abdullahkhan-hw3-bucket")
files = bucket.list_blobs()

for blob in files:
    fname = blob.name
    blob.download_to_filename(fname)
    print('Downloading '+fname+ ' from '+bucket.name)
