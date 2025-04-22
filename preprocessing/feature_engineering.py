
import pandas as pd
import json
from google.cloud import storage, bigquery
import io
import hashlib
import os

# Load config
with open('config/gcp_config.json') as f:
    gcp_config = json.load(f)

project_id = gcp_config["project_id"]
bucket_name = gcp_config["bucket"]
gcs_input_path = gcp_config["gcs_input_path"]
bq_dataset = gcp_config["bq_dataset"]
bq_table = gcp_config["bq_output_table"]

# GCP Clients
storage_client = storage.Client(project=project_id)
bucket = storage_client.bucket(bucket_name)
bq_client = bigquery.Client(project=project_id)

print("\n🔍 Searching for latest GCS files...")

def get_latest_blob(prefix):
    blobs = list(bucket.list_blobs(prefix="raw/"))
    matching = [b for b in blobs if b.name.startswith(f"raw/{prefix}_") and b.name.endswith(".csv")]
    if not matching:
        raise FileNotFoundError(f"No GCS files for raw/{prefix}_*.csv")
    return sorted(matching, key=lambda b: b.updated, reverse=True)[0]

def blob_to_df(blob):
    print(f"📥 Streaming {blob.name} from GCS...")
    stream = io.BytesIO()
    blob.download_to_file(stream)
    stream.seek(0)
    return pd.read_csv(stream)

# Download CSVs from GCS to memory
mta_df = blob_to_df(get_latest_blob("mta"))
mode_df = blob_to_df(get_latest_blob("mode"))

# Hash for change detection
def df_hash(df): return hashlib.md5(pd.util.hash_pandas_object(df, index=True).values).hexdigest()
hash_combined = hashlib.md5((df_hash(mta_df) + df_hash(mode_df)).encode()).hexdigest()

hash_blob = bucket.blob("hashes/last_feature_merge.hash")
if hash_blob.exists() and hash_blob.download_as_text().strip() == hash_combined:
    print("⏭️  Skipping: data unchanged.")
    exit(0)

print("🔧 Preprocessing...")
if 'date' in mta_df.columns:
    mta_df['date'] = pd.to_datetime(mta_df['date'])
mta_df.fillna(0, inplace=True)
mode_df.fillna(method='ffill', inplace=True)

if 'weather' in mode_df.columns:
    mode_df['weather_encoded'] = mode_df['weather'].astype('category').cat.codes
if 'mode' in mode_df.columns:
    mode_df['mode_encoded'] = mode_df['mode'].astype('category').cat.codes

if 'date' in mta_df.columns and 'date' in mode_df.columns:
    merged_df = pd.merge(mta_df, mode_df, on='date', how='inner')
else:
    merged_df = pd.concat([mta_df, mode_df], axis=1)

if 'fare' in merged_df.columns and 'duration' in merged_df.columns:
    merged_df['fare_per_minute'] = merged_df['fare'] / (merged_df['duration'] + 1)

# Upload CSV to GCS
print("☁️ Uploading merged dataset to GCS...")
csv_buf = io.StringIO()
merged_df.to_csv(csv_buf, index=False)
csv_buf.seek(0)
bucket.blob(gcs_input_path).upload_from_string(csv_buf.getvalue(), content_type="text/csv")

# Update hash
hash_blob.upload_from_string(hash_combined)

# Upload to BigQuery
print("📤 Uploading to BigQuery...")
dataset_ref = bigquery.DatasetReference(project_id, bq_dataset)
try:
    bq_client.get_dataset(dataset_ref)
except:
    bq_client.create_dataset(bigquery.Dataset(dataset_ref))
    print(f"✅ Created BigQuery dataset: {bq_dataset}")

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1
)

load_job = bq_client.load_table_from_file(io.StringIO(csv_buf.getvalue()), f"{project_id}.{bq_dataset}.{bq_table}", job_config=job_config)
load_job.result()
print(f"✅ BigQuery table {bq_dataset}.{bq_table} loaded.")
