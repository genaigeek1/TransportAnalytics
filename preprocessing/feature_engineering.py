
import pandas as pd
import json
from google.cloud import storage, bigquery
import os
from datetime import datetime
import hashlib

# Load config
with open('config/gcp_config.json') as f:
    gcp_config = json.load(f)

project_id = gcp_config["project_id"]
bucket_name = gcp_config["bucket"]
gcs_input_path = gcp_config["gcs_input_path"]
bq_dataset = gcp_config["bq_dataset"]
bq_table = gcp_config["bq_output_table"]

# Setup GCS + BQ client
storage_client = storage.Client(project=project_id)
bucket = storage_client.bucket(bucket_name)
bq_client = bigquery.Client(project=project_id)

# Helper: get latest blob for a dataset type
print("\n🔍 Searching for latest GCS files...")
def get_latest_blob(prefix):
    blobs = list(bucket.list_blobs(prefix="raw/"))
    matching = [b for b in blobs if b.name.startswith(f"raw/{prefix}_") and b.name.endswith(".csv")]
    if not matching:
        raise FileNotFoundError(f"No matching GCS files found for prefix: raw/{prefix}_*.csv")
    return sorted(matching, key=lambda b: b.updated, reverse=True)[0]

# Download MTA and Mode files
try:
    latest_mta_blob = get_latest_blob("mta")
except Exception as e:
    print(f"❌ Failed to find MTA raw file in GCS: {str(e)}"); exit(1)
try:
    latest_mode_blob = get_latest_blob("mode")
except Exception as e:
    print(f"❌ Failed to find Mode Choice raw file in GCS: {str(e)}"); exit(1)

local_mta_path = "/tmp/mta_data.csv"
local_mode_path = "/tmp/mode_data.csv"

print("⬇️  Downloading MTA data...")
latest_mta_blob.download_to_filename(local_mta_path)
print("⬇️  Downloading Mode Choice data...")
latest_mode_blob.download_to_filename(local_mode_path)

print(f"✅ Downloaded: {latest_mta_blob.name}")
print(f"✅ Downloaded: {latest_mode_blob.name}")

# Compute combined hash of both files to detect duplication
def file_md5(path):
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

mta_hash = file_md5(local_mta_path)
mode_hash = file_md5(local_mode_path)
combined_hash = hashlib.md5((mta_hash + mode_hash).encode()).hexdigest()

hash_blob = bucket.blob("hashes/last_feature_merge.hash")

# Check if hash already processed
if hash_blob.exists() and hash_blob.download_as_text().strip() == combined_hash:
    print("⏭️  Same input data as last run. Skipping feature merge and upload.")
    exit(0)

# Load datasets
print("📊 Reading MTA data into DataFrame...")
try:
    mta_df = pd.read_csv(local_mta_path)
except Exception as e:
    print(f"❌ Failed to read MTA data: {str(e)}"); exit(1)
print("📊 Reading Mode Choice data into DataFrame...")
try:
    mode_df = pd.read_csv(local_mode_path)
except Exception as e:
    print(f"❌ Failed to read Mode Choice data: {str(e)}"); exit(1)

# Preprocess
if 'date' in mta_df.columns:
    mta_df['date'] = pd.to_datetime(mta_df['date'])
mta_df.fillna(0, inplace=True)

mode_df.fillna(method='ffill', inplace=True)
if 'weather' in mode_df.columns:
    mode_df['weather_encoded'] = mode_df['weather'].astype('category').cat.codes
if 'mode' in mode_df.columns:
    mode_df['mode_encoded'] = mode_df['mode'].astype('category').cat.codes

# Merge
if 'date' in mta_df.columns and 'date' in mode_df.columns:
    print("🔗 Merging datasets on date...")
try:
    merged_df = pd.merge(mta_df, mode_df, on='date', how='inner')
except Exception as e:
    print(f"❌ Failed to merge datasets on 'date': {str(e)}"); exit(1)
else:
    merged_df = pd.concat([mta_df, mode_df], axis=1)

# Feature engineering
if 'fare' in merged_df.columns and 'duration' in merged_df.columns:
    merged_df['fare_per_minute'] = merged_df['fare'] / (merged_df['duration'] + 1)

# Save + upload merged file
output_local = "/tmp/merged_feature_data.csv"
print("💾 Saving merged dataset locally...")
merged_df.to_csv(output_local, index=False)
print(f"☁️  Uploading merged dataset to GCS at: gs://{bucket_name}/{gcs_input_path}")
try:
    bucket.blob(gcs_input_path).upload_from_filename(output_local)
except Exception as e:
    print(f"❌ Failed to upload merged dataset to GCS: {str(e)}"); exit(1)
print(f"✅ Uploaded merged data to: gs://{bucket_name}/{gcs_input_path}")

# Save current hash
print("💾 Saving input hash to GCS to track changes...")
hash_blob.upload_from_string(combined_hash)

# Validate or create BQ dataset
dataset_ref = bigquery.DatasetReference(project_id, bq_dataset)
try:
    bq_client.get_dataset(dataset_ref)
except:
    print(f"⚠️ BigQuery dataset {bq_dataset} not found. Creating...")
    print("📁 Creating BigQuery dataset...")
bq_client.create_dataset(bigquery.Dataset(dataset_ref))
    print(f"✅ Created BigQuery dataset: {bq_dataset}")

# Upload merged data to BigQuery
table_ref = f"{project_id}.{bq_dataset}.{bq_table}"
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1
)

with open(output_local, "rb") as source_file:
    print("📤 Uploading data to BigQuery...")
load_job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)
    try:
    load_job.result()
except Exception as e:
    print(f"❌ BigQuery load job failed: {str(e)}"); exit(1)

print(f"✅ Uploaded to BigQuery table: {table_ref}")
