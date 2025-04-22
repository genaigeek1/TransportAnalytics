
import os
import argparse
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
import zipfile
import io

# CLI arguments
parser = argparse.ArgumentParser(description="Stream Kaggle datasets directly to GCS.")
parser.add_argument("--project_id", default="gps-ax-lakehouse", help="GCP Project ID")
parser.add_argument("--bucket_name", default="mta-ridership-data", help="GCS Bucket name")
args = parser.parse_args()

# Kaggle credentials from environment
kaggle_username = os.getenv("KAGGLE_USERNAME")
kaggle_key = os.getenv("KAGGLE_KEY")

if not kaggle_username or not kaggle_key:
    raise ValueError("❌ Set KAGGLE_USERNAME and KAGGLE_KEY in environment.")

os.environ["KAGGLE_USERNAME"] = kaggle_username
os.environ["KAGGLE_KEY"] = kaggle_key

# Authenticate
api = KaggleApi()
api.authenticate()
print(f"✅ Authenticated as {kaggle_username}")

# Datasets
datasets = {
    "mta": "princehobby/metropolitan-transportation-authority-mta-datasets",
    "mode": "soumyadiptadas/multimodal-transport-dataset"
}

# GCS setup
storage_client = storage.Client(project=args.project_id)
bucket = storage_client.bucket(args.bucket_name)

if not bucket.exists():
    print(f"⚠️ Bucket {args.bucket_name} does not exist. Creating...")
    bucket = storage_client.create_bucket(args.bucket_name, location="us-central1")
    print(f"✅ Created bucket: {args.bucket_name}")

# Download and stream-upload datasets
for prefix, dataset in datasets.items():
    print(f"⬇️ Downloading dataset: {dataset}")
    data = api.dataset_download_files(dataset, path=None, unzip=False)
    with zipfile.ZipFile(io.BytesIO(data.content)) as archive:
        for name in archive.namelist():
            if name.endswith(".csv"):
                print(f"☁️ Uploading {name} to GCS as raw/{prefix}_{name}")
                blob = bucket.blob(f"raw/{prefix}_{os.path.basename(name)}")
                with archive.open(name) as f:
                    blob.upload_from_file(f, rewind=True)
print("✅ All files streamed to GCS successfully.")
