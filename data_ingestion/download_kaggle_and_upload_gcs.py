
import os
import argparse
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage

# CLI arguments
parser = argparse.ArgumentParser(description="Download Kaggle dataset and upload to GCS.")
parser.add_argument("--username", required=True, help="Kaggle username")
parser.add_argument("--key", required=True, help="Kaggle API key")
parser.add_argument("--project_id", default="gps-ax-lakehouse", help="GCP Project ID")
parser.add_argument("--bucket_name", default="mta-ridership-data", help="GCS Bucket name")
parser.add_argument("--dataset", default="princehobby/metropolitan-transportation-authority-mta-datasets", help="Kaggle dataset slug")
parser.add_argument("--download_dir", default="mta_data", help="Local folder to store downloaded files")
args = parser.parse_args()

# Set credentials
os.environ["KAGGLE_USERNAME"] = args.username
os.environ["KAGGLE_KEY"] = args.key

# Download from Kaggle
api = KaggleApi()
api.authenticate()
api.dataset_download_files(args.dataset, path=args.download_dir, unzip=True)

# Upload to GCS
client = storage.Client(project=args.project_id)
bucket = client.bucket(args.bucket_name)

for file in os.listdir(args.download_dir):
    if file.endswith(".csv"):
        blob = bucket.blob(f"raw/{file}")
        blob.upload_from_filename(os.path.join(args.download_dir, file))
        print(f"✅ Uploaded {file} to gs://{args.bucket_name}/raw/{file}")
