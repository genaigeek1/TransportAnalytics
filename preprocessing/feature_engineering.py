
import pandas as pd
import json
from google.cloud import storage
import os

# Load config
with open('config/gcp_config.json') as f:
    gcp_config = json.load(f)

# Simulate merging logic — this would typically come from your real logic
df = pd.read_csv("preprocessing/merged_feature_data.csv")  # Assume you have this locally or from prior steps

# Upload to GCS for managed dataset usage
client = storage.Client(project=gcp_config["project_id"])
bucket = client.bucket(gcp_config["bucket"])
gcs_blob_path = gcp_config["gcs_input_path"]
local_output_path = "/tmp/merged_feature_data.csv"

df.to_csv(local_output_path, index=False)
bucket.blob(gcs_blob_path).upload_from_filename(local_output_path)

print(f"Merged feature data uploaded to gs://{gcp_config['bucket']}/{gcs_blob_path}")
