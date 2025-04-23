import os
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import requests
from io import BytesIO
import zipfile
import pandas as pd  # For basic XLSX handling (requires openpyxl: pip install openpyxl)
import json

# Kaggle API credentials (ensure these are set as environment variables or configured)
os.environ['KAGGLE_USERNAME'] = 'your_kaggle_username'  # Replace with your Kaggle username
os.environ['KAGGLE_KEY'] = 'your_kaggle_api_key'      # Replace with your Kaggle API key

# Google Cloud Storage details
gcs_bucket_name = 'mta-ridership-data'  # Replace with your GCS bucket name

# Kaggle dataset details
kaggle_dataset_slug_mta = "princehobby/metropolitan-transportation-authority-mta-datasets"
kaggle_dataset_slug_multimodal = "merdelic/dataset-for-multimodal-transport-analytics"

# Initialize Kaggle API
api = KaggleApi()
api.authenticate()

def upload_to_gcs(bucket_name, blob_name, file_content):
    """Uploads content to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(file_content)
    print(f"✅ Uploaded to gs://{bucket_name}/{blob_name}")

def process_and_upload(kaggle_slug, gcs_prefix):
    """Downloads a suitable data file from Kaggle and uploads it to GCS."""
    print(f"\nProcessing dataset: https://www.kaggle.com/{kaggle_slug}")
    try:
        dataset_files = api.dataset_list_files(kaggle_slug).files
        if dataset_files:
            suitable_files = [f for f in dataset_files if f.name.endswith(('.csv', '.txt', '.xlsx', '.json', '.parquet', '.zip'))]
            if suitable_files:
                for file_info in suitable_files:
                    file_name = file_info.name
                    download_url = f"https://www.kaggle.com/api/v1/datasets/download/{kaggle_slug}/{file_name}"
                    response = requests.get(download_url, headers={'Authorization': f'Bearer {os.environ['KAGGLE_KEY']}'}, stream=True)
                    response.raise_for_status()
                    file_content_stream = BytesIO(response.content)
                    gcs_blob_name = f"{gcs_prefix}/{file_name}"

                    print(f"📤 Attempting to upload {file_name} to gs://{gcs_bucket_name}/{gcs_blob_name}")

                    if file_name.endswith('.zip'):
                        try:
                            with zipfile.ZipFile(file_content_stream) as zf:
                                for name in zf.namelist():
                                    if name.endswith(('.csv', '.txt', '.xlsx', '.json', '.parquet')):
                                        print(f"  - Found {name} inside zip, uploading...")
                                        with zf.open(name) as inner_file:
                                            upload_to_gcs(gcs_bucket_name, f"{gcs_prefix}/{os.path.splitext(file_name)[0]}_{name}", BytesIO(inner_file.read()))
                        except zipfile.BadZipFile:
                            print(f"⚠️ Could not open {file_name} as a zip file.")
                    elif file_name.endswith('.xlsx'):
                        try:
                            df = pd.read_excel(file_content_stream)
                            csv_buffer = BytesIO()
                            df.to_csv(csv_buffer, index=False)
                            csv_buffer.seek(0)
                            upload_to_gcs(gcs_bucket_name, f"{gcs_prefix}/{os.path.splitext(file_name)[0]}.csv", csv_buffer)
                        except Exception as e:
                            print(f"⚠️ Error processing XLSX file {file_name}: {e}")
                    else:
                        upload_to_gcs(gcs_bucket_name, gcs_blob_name, file_content_stream)
            else:
                print(f"⚠️ No suitable data file (csv, txt, xlsx, json, parquet, zip) found in {kaggle_slug}.")
        else:
            print(f"⚠️ Could not retrieve file list for {kaggle_slug}")
    except requests.exceptions.HTTPError as e:
        print(f"❌ Error processing {kaggle_slug}: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred while processing {kaggle_slug}: {e}")

# Process MTA dataset
process_and_upload(kaggle_dataset_slug_mta, 'raw/mta')

# Process Multimodal dataset
process_and_upload(kaggle_dataset_slug_multimodal, 'raw/multimodal')

print("\n✅ Script finished processing datasets.")