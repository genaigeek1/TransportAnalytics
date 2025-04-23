# download_kaggle_and_upload_gcs.py
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import requests
from io import BytesIO
import posixpath  # For platform-independent path manipulation

# Kaggle API credentials (ensure these are set as environment variables or configured)
os.environ['KAGGLE_USERNAME'] = 'your_kaggle_username'  # Replace with your Kaggle username
os.environ['KAGGLE_KEY'] = 'your_kaggle_api_key'      # Replace with your Kaggle API key

# Google Cloud Storage details
gcs_bucket_name = 'mta-ridership-data'  # Replace with your GCS bucket name
gcs_multimodal_prefix = 'raw/multimodal/archive/' # GCS prefix for the archive files

# Kaggle dataset details
kaggle_dataset_slug_mta = "princehobby/metropolitan-transportation-authority-mta-datasets"
kaggle_dataset_slug_multimodal = "merdelic/dataset-for-multimodal-transport-analytics"
target_multimodal_subdir = "Collecty data/archive"

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

def process_and_upload_mta(kaggle_slug, gcs_prefix):
    """Downloads and uploads MTA dataset files to GCS."""
    print(f"\nProcessing MTA dataset: https://www.kaggle.com/{kaggle_slug}")
    try:
        dataset_files = api.dataset_list_files(kaggle_slug).files
        if dataset_files:
            for file_info in dataset_files:
                file_name = file_info.name
                download_url = f"https://www.kaggle.com/api/v1/datasets/download/{kaggle_slug}/{file_name}"
                response = requests.get(download_url, headers={'Authorization': f'Bearer {os.environ['KAGGLE_KEY']}'}, stream=True)
                response.raise_for_status()
                file_content_stream = BytesIO(response.content)
                gcs_blob_name = f"{gcs_prefix}/{file_name}"
                upload_to_gcs(gcs_bucket_name, gcs_blob_name, file_content_stream)
        else:
            print(f"⚠️ Could not retrieve file list for {kaggle_slug}")
    except requests.exceptions.HTTPError as e:
        print(f"❌ Error processing {kaggle_slug}: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred while processing {kaggle_slug}: {e}")

def process_and_upload_multimodal_subdir(kaggle_slug, gcs_prefix, target_subdir):
    """Downloads all files from a specific subdirectory in Kaggle and uploads them to GCS using streaming."""
    print(f"\nProcessing multimodal subdirectory '{target_subdir}' from https://www.kaggle.com/{kaggle_slug}")
    try:
        dataset_files = api.dataset_list_files(kaggle_slug).files
        if dataset_files:
            for file_info in dataset_files:
                if file_info.name.startswith(target_subdir):
                    file_name_on_kaggle = file_info.name
                    gcs_blob_name = posixpath.join(gcs_prefix, os.path.basename(file_name_on_kaggle))
                    download_url = f"https://www.kaggle.com/api/v1/datasets/download/{kaggle_slug}/{file_name_on_kaggle}"
                    try:
                        response = requests.get(download_url, headers={'Authorization': f'Bearer {os.environ['KAGGLE_KEY']}'}, stream=True)
                        response.raise_for_status()

                        client = storage.Client()
                        bucket = client.bucket(gcs_bucket_name)
                        blob = bucket.blob(gcs_blob_name)

                        print(f"📤 Uploading {file_name_on_kaggle} to gs://{gcs_bucket_name}/{gcs_blob_name}")
                        blob.upload_from_file(response.raw)
                        print(f"✅ Uploaded {file_name_on_kaggle} to GCS")

                    except requests.exceptions.HTTPError as e:
                        print(f"❌ Error downloading {file_name_on_kaggle} from {kaggle_slug}: {e}")
                    except Exception as e:
                        print(f"❌ An unexpected error occurred during upload of {file_name_on_kaggle}: {e}")
        else:
            print(f"⚠️ Could not retrieve file list for {kaggle_slug}")
    except requests.exceptions.HTTPError as e:
        print(f"❌ Error processing subdirectory '{target_subdir}' from {kaggle_slug}: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred while processing subdirectory '{target_subdir}' from {kaggle_slug}: {e}")

# Process MTA dataset
process_and_upload_mta(kaggle_dataset_slug_mta, 'raw/mta')

# Process all files in the specified multimodal subdirectory
process_and_upload_multimodal_subdir(kaggle_dataset_slug_multimodal, gcs_multimodal_prefix, target_multimodal_subdir)

print("\n✅ Script finished processing datasets.")