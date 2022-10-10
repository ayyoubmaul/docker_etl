from google.cloud import storage
import os
import glob


CWD = os.path.dirname(__file__)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(CWD, "service_account.json")

def upload_to_bucket(bucket_name, **kwargs):
    """ Upload data to a bucket"""
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    
    for file_name in glob('flights_tickets_serp*.csv'):
        blob = bucket.blob(file_name)

        blob.upload_from_filename(file_name)
   
        blob_url = blob.public_url

        # remove file after success upload
        if kwargs['remove_file'] == True:
            os.remove(file_name)
    # returns a public url
    return blob_url
