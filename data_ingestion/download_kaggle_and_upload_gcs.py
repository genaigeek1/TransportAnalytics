import os
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import requests
from io import BytesIO
import zipfile

# Kaggle API credentials (ensure these are set as environment variables or configured)
os.environ['KAGGLE_USERNAME'] = 'your_kaggle_username'  # Replace with your Kaggle username
os.environ['KAGGLE_KEY'] = 'your_kaggle_api_key'      # Replace with your Kaggle API key

# Google Cloud Storage details
gcs_bucket_name = 'mta-ridership-data'  # Replace with your GCS bucket name
gcs_blob_name_mta = 'raw/mta_data.csv'       # Desired path and filename for MTA in GCS
gcs_blob_name_multimodal = 'raw/multimodal_transport_data.csv' # Desired path and filename for multimodal in GCS

# Kaggle dataset details
kaggle_dataset_slug_mta = "princehobby/metropolitan-transportation-authority-mta-datasets"
kaggle_dataset_slug_multimodal = "merdelic/dataset-for-multimodal-transport-analytics"

# Initialize Kaggle API
api = KaggleApi()
api.authenticate()

# --- Directly Download and Upload MTA Dataset to GCS ---
print(f"Dataset URL: https://www.kaggle.com/{kaggle_dataset_slug_mta}")

try:
    dataset_files_mta = api.dataset_list_files(kaggle_dataset_slug_mta).files
    if dataset_files_mta:
        file_to_upload_mta = next((f.name for f in dataset_files_mta if f.name.endswith(('.csv', '.zip'))), None)
        if file_to_upload_mta:
            download_url_mta = f"https://www.kaggle.com/api/v1/datasets/download/{kaggle_dataset_slug_mta}/{file_to_upload_mta}"
            response_mta = requests.get(download_url_mta, headers={'Authorization': f'Bearer {os.environ["KAGGLE_KEY"]}'}, stream=True)
            response_mta.raise_for_status()

            client = storage.Client()
            bucket = client.bucket(gcs_bucket_name)
            blob_mta = bucket.blob(gcs_blob_name_mta)

            print(f"📤 Uploading {file_to_upload_mta} to gs://{gcs_bucket_name}/{gcs_blob_name_mta}")

            if file_to_upload_mta.endswith('.zip'):
                with zipfile.ZipFile(BytesIO(response_mta.content)) as zf:
                    csv_file_name_mta = next((name for name in zf.namelist() if name.endswith('.csv')), None)
                    if csv_file_name_mta:
                        with zf.open(csv_file_name_mta) as csv_file:
                            blob_mta.upload_from_file(csv_file)
                        print(f"✅ Uploaded {csv_file_name_mta} from zip to GCS")
                    else:
                        print("⚠️ No CSV file found inside the MTA zip.")
            else:
                blob_mta.upload_from_file(BytesIO(response_mta.content))
                print("✅ Uploaded MTA dataset directly to GCS")
        else:
            print("⚠️ No suitable data file (csv or zip) found in the MTA dataset.")
    else:
        print(f"⚠️ Could not retrieve file list for {kaggle_dataset_slug_mta}")

except requests.exceptions.HTTPError as e:
    print(f"❌ Error downloading MTA dataset: {e}")
except Exception as e:
    print(f"❌ An error occurred during MTA dataset processing: {e}")

# --- Directly Download and Upload Multimodal Dataset to GCS ---
print(f"\nDataset URL: https://www.kaggle.com/{kaggle_dataset_slug_multimodal}")

try:
    dataset_files_multimodal = api.dataset_list_files(kaggle_dataset_slug_multimodal).files
    if dataset_files_multimodal:
        file_to_upload_multimodal = next((f.name for f in dataset_files_multimodal if f.name.endswith(('.csv', '.zip'))), None)
        if file_to_upload_multimodal:
            download_url_multimodal = f"https://www.kaggle.com/api/v1/datasets/download/{kaggle_dataset_slug_multimodal}/{file_to_upload_multimodal}"
            response_multimodal = requests.get(download_url_multimodal, headers={'Authorization': f'Bearer {os.environ["KAGGLE_KEY"]}'}, stream=True)
            response_multimodal.raise_for_status()

            client = storage.Client()
            bucket = client.bucket(gcs_bucket_name)
            blob_multimodal = bucket.blob(gcs_blob_name_multimodal)

            print(f"📤 Uploading {file_to_upload_multimodal} to gs://{gcs_bucket_name}/{gcs_blob_name_multimodal}")

            if file_to_upload_multimodal.endswith('.zip'):
                with zipfile.ZipFile(BytesIO(response_multimodal.content)) as zf:
                    csv_file_name_multimodal = next((name for name in zf.namelist() if name.endswith('.csv')), None)
                    if csv_file_name_multimodal:
                        with zf.open(csv_file_name_multimodal) as csv_file:
                            blob_multimodal.upload_from_file(csv_file)
                        print(f"✅ Uploaded {csv_file_name_multimodal} from zip to GCS")
                    else:
                        print("⚠️ No CSV file found inside the multimodal zip.")
            else:
                blob_multimodal.upload_from_file(BytesIO(response_multimodal.content))
                print("✅ Uploaded multimodal dataset directly to GCS")
        else:
            print("⚠️ No suitable data file (csv or zip) found in the multimodal dataset.")
    else:
        print(f"⚠️ Could not retrieve file list for {kaggle_dataset_slug_multimodal}")

except requests.exceptions.HTTPError as e:
    print(f"❌ Error downloading multimodal dataset: {e}")
except Exception as e:
    print(f"❌ An error occurred during multimodal dataset processing: {e}")