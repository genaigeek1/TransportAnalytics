
import os
import argparse
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage

# CLI arguments
parser = argparse.ArgumentParser(description="Download Kaggle dataset and upload to GCS.")
parser.add_argument("--project_id", default="gps-ax-lakehouse", help="GCP Project ID")
parser.add_argument("--bucket_name", default="mta-ridership-data", help="GCS Bucket name")
parser.add_argument("--dataset", default="princehobby/metropolitan-transportation-authority-mta-datasets", help="Kaggle dataset slug")
parser.add_argument("--download_dir", default="mta_data", help="Local folder to store downloaded files")
args = parser.parse_args()

# Set credentials from environment
kaggle_username = os.getenv("KAGGLE_USERNAME")
kaggle_key = os.getenv("KAGGLE_KEY")

if not kaggle_username or not kaggle_key:
    raise ValueError("❌ Environment variables KAGGLE_USERNAME and KAGGLE_KEY must be set.")

os.environ["KAGGLE_USERNAME"] = kaggle_username
os.environ["KAGGLE_KEY"] = kaggle_key

# Authenticate with Kaggle API
api = KaggleApi()
api.authenticate()
print(f"✅ Authenticated with Kaggle as {kaggle_username}")

# Download the dataset
api.dataset_download_files(args.dataset, path=args.download_dir, unzip=True)
print(f"✅ Downloaded dataset: {args.dataset} to {args.download_dir}")

# Upload to GCS
client = storage.Client(project=args.project_id)
bucket = client.bucket(args.bucket_name)

if not bucket.exists():
    print(f"⚠️ Bucket {args.bucket_name} does not exist. Creating it...")
    bucket = client.create_bucket(args.bucket_name, location="us-central1")
    print(f"✅ Created new bucket: gs://{args.bucket_name}")

for file in os.listdir(args.download_dir):
    if file.endswith(".csv"):
        blob = bucket.blob(f"raw/{file}")
        blob.upload_from_filename(os.path.join(args.download_dir, file))
        print(f"✅ Uploaded {file} to gs://{args.bucket_name}/raw/{file}")
