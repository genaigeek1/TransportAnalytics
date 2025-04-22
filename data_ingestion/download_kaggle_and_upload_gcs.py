
import os
import argparse
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage

# CLI arguments
parser = argparse.ArgumentParser(description="Download Kaggle dataset files individually and upload to GCS.")
parser.add_argument("--project_id", default="gps-ax-lakehouse", help="GCP Project ID")
parser.add_argument("--bucket_name", default="mta-ridership-data", help="GCS Bucket name")
args = parser.parse_args()

# Set Kaggle credentials
kaggle_username = os.getenv("KAGGLE_USERNAME")
kaggle_key = os.getenv("KAGGLE_KEY")

if not kaggle_username or not kaggle_key:
    raise ValueError("❌ KAGGLE_USERNAME and KAGGLE_KEY must be set as environment variables.")

os.environ["KAGGLE_USERNAME"] = kaggle_username
os.environ["KAGGLE_KEY"] = kaggle_key

# Authenticate
api = KaggleApi()
api.authenticate()
print(f"✅ Authenticated as {kaggle_username}")

# Datasets to process
datasets = {
    "mta": "princehobby/metropolitan-transportation-authority-mta-datasets",
    "mode": "merdelic/dataset-for-multimodal-transport-analytics"
}

# Initialize GCS client
storage_client = storage.Client(project=args.project_id)
bucket = storage_client.bucket(args.bucket_name)

if not bucket.exists():
    print(f"⚠️ Bucket {args.bucket_name} does not exist. Creating...")
    bucket = storage_client.create_bucket(args.bucket_name, location="us-central1")
    print(f"✅ Created bucket: {args.bucket_name}")

# Process each dataset
for prefix, dataset_slug in datasets.items():
    print(f"🔍 Listing files for {dataset_slug}...")
    files = api.dataset_list_files(dataset_slug).files

    for file_info in files:
        file_name = file_info.name
        if file_name.endswith(".csv"):
            local_path = f"/tmp/{file_name}"
            print(f"⬇️ Downloading {file_name} from Kaggle...")
            api.dataset_download_file(dataset_slug, file_name, path="/tmp", force=True)
            blob_path = f"raw/{prefix}_{os.path.basename(file_name)}"
            print(f"☁️ Uploading {file_name} to gs://{args.bucket_name}/{blob_path}")
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            os.remove(local_path)

print("✅ All eligible CSV files have been downloaded and streamed to GCS.")
