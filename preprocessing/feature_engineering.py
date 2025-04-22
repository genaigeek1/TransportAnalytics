
import pandas as pd
import json
from google.cloud import storage
import os

# Load config
with open('config/gcp_config.json') as f:
    gcp_config = json.load(f)

project_id = gcp_config["project_id"]
bucket_name = gcp_config["bucket"]

# Setup GCS client
client = storage.Client(project=project_id)
bucket = client.bucket(bucket_name)

# Download raw MTA and mode choice datasets from GCS
raw_mta_path = "raw/mta_data.csv"
raw_mode_path = "raw/mode_choice_data.csv"

local_mta_path = "/tmp/mta_data.csv"
local_mode_path = "/tmp/mode_choice_data.csv"

bucket.blob(raw_mta_path).download_to_filename(local_mta_path)
bucket.blob(raw_mode_path).download_to_filename(local_mode_path)

# Load datasets
mta_df = pd.read_csv(local_mta_path)
mode_df = pd.read_csv(local_mode_path)

# Clean & prepare MTA data
if 'date' in mta_df.columns:
    mta_df['date'] = pd.to_datetime(mta_df['date'])
mta_df.fillna(0, inplace=True)

# Prepare mode choice data
mode_df.fillna(method='ffill', inplace=True)
if 'weather' in mode_df.columns:
    mode_df['weather_encoded'] = mode_df['weather'].astype('category').cat.codes
if 'mode' in mode_df.columns:
    mode_df['mode_encoded'] = mode_df['mode'].astype('category').cat.codes

# Merge datasets on date
if 'date' in mta_df.columns and 'date' in mode_df.columns:
    merged_df = pd.merge(mta_df, mode_df, on='date', how='inner')
else:
    merged_df = pd.concat([mta_df, mode_df], axis=1)

# Feature engineering
if 'fare' in merged_df.columns and 'duration' in merged_df.columns:
    merged_df['fare_per_minute'] = merged_df['fare'] / (merged_df['duration'] + 1)

# Save and upload processed dataset to GCS
processed_path_local = "/tmp/merged_feature_data.csv"
processed_path_gcs = gcp_config["gcs_input_path"]

merged_df.to_csv(processed_path_local, index=False)
bucket.blob(processed_path_gcs).upload_from_filename(processed_path_local)

print(f"✅ Processed dataset uploaded to: gs://{bucket_name}/{processed_path_gcs}")
