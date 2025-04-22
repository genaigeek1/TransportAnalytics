
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage

# Set your Kaggle credentials as environment variables or set manually
os.environ['KAGGLE_USERNAME'] = 'your_kaggle_username'
os.environ['KAGGLE_KEY'] = 'your_kaggle_key'

# Dataset details
dataset_slug = 'princehobby/metropolitan-transportation-authority-mta-datasets'
download_dir = 'mta_data'

# Download from Kaggle
api = KaggleApi()
api.authenticate()
api.dataset_download_files(dataset_slug, path=download_dir, unzip=True)

# Upload to GCS
project_id = 'gps-ax-lakehouse'
bucket_name = 'mta-ridership-data'

client = storage.Client(project=project_id)
bucket = client.bucket(bucket_name)

for file in os.listdir(download_dir):
    if file.endswith('.csv'):
        blob = bucket.blob(f"raw/{file}")
        blob.upload_from_filename(os.path.join(download_dir, file))
        print(f"Uploaded {file} to gs://{bucket_name}/raw/{file}")
