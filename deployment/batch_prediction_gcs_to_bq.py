
import os
import pandas as pd
from google.cloud import storage, bigquery
import joblib
from datetime import datetime

# CONFIG
project_id = "gps-ax-lakehouse"
bucket_name = "mta-ridership-data"
model_blob_path = "models/ridership_model.pkl"
input_blob_path = "inputs/merged_feature_data.csv"
bq_dataset = "ridership_analytics"
bq_table = "batch_predictions"

# Init clients
storage_client = storage.Client(project=project_id)
bq_client = bigquery.Client(project=project_id)

# Download model from GCS
model_local_path = "/tmp/ridership_model.pkl"
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(model_blob_path)
blob.download_to_filename(model_local_path)

# Load model
model = joblib.load(model_local_path)

# Download input data from GCS
input_local_path = "/tmp/merged_feature_data.csv"
bucket.blob(input_blob_path).download_to_filename(input_local_path)
df = pd.read_csv(input_local_path)

# Run predictions
features = ["weather_encoded", "duration", "fare", "fare_per_minute"]
df["predicted_ridership"] = model.predict(df[features])
df["batch_run_time"] = datetime.utcnow()

# Define BigQuery table schema (auto-detect)
table_id = f"{project_id}.{bq_dataset}.{bq_table}"

# Upload to BigQuery
job = bq_client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"
))
job.result()
print(f"✅ Predictions uploaded to BigQuery table: {table_id}")
