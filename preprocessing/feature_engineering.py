import pandas as pd
from google.cloud import storage
import io
import os
from datetime import datetime

# Google Cloud Storage details
gcs_bucket_name = 'mta-ridership-data'  # Replace with your GCS bucket name
gcs_mta_prefix = 'raw/mta/'          # Prefix for MTA data
gcs_multimodal_prefix = 'raw/multimodal/archive/' # Prefix for multimodal archive data
gcs_processed_blob_name = 'processed/merged_transport_data.csv' # Blob name for final processed data

def read_csv_from_gcs(bucket_name, blob_name):
    """Reads a single CSV file from GCS into a DataFrame."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content))
        print(f"✅ Read data from gs://{bucket_name}/{blob_name}")
        return df
    except Exception as e:
        print(f"❌ Error reading {blob_name}: {e}")
        return None

def read_txt_from_gcs(bucket_name, blob_name, sep='\t', header=None):
    """Reads a single TXT file from GCS into a DataFrame."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        content = blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(content), sep=sep, header=header)
        print(f"✅ Read data from gs://{bucket_name}/{blob_name}")
        return df
    except Exception as e:
        print(f"❌ Error reading {blob_name}: {e}")
        return None

def preprocess_mta_data(mta_dfs):
    """Preprocesses the list of MTA DataFrames."""
    if not mta_dfs:
        print("No MTA data to preprocess.")
        return None
    mta_df = pd.concat(mta_dfs, ignore_index=True)
    # Convert 'DATE' and 'TIME' to a single datetime column
    if 'DATE' in mta_df.columns and 'TIME' in mta_df.columns:
        mta_df['DATETIME'] = pd.to_datetime(mta_df['DATE'] + ' ' + mta_df['TIME'], errors='coerce')
        mta_df = mta_df.drop(columns=['DATE', 'TIME'])
    elif 'DATETIME' in mta_df.columns:
        mta_df['DATETIME'] = pd.to_datetime(mta_df['DATETIME'], errors='coerce')

    # Basic filtering (you might need more sophisticated filtering)
    numeric_cols = mta_df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        mta_df = mta_df[(mta_df[col] >= 0) & (mta_df[col] < 1000000)] # Basic outlier removal

    return mta_df

def preprocess_multimodal_data(bucket_name, prefix):
    """Preprocesses the multimodal data from GCS (assuming multiple TXT files)."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    all_multimodal_dfs = []
    for blob in blobs:
        if blob.name.endswith('.txt'):
            print(f"Reading and preprocessing multimodal data from gs://{bucket_name}/{blob.name}")
            df = read_txt_from_gcs(bucket_name, blob.name, sep=',', header='infer') # Adjust separator and header as needed
            if df is not None:
                # **Adapt this preprocessing based on the structure of your TXT files**
                # Example: Assuming columns like 'user_id', 'mode', 'timestamp'
                if df.shape[1] >= 3:
                    df.columns = ['user_id', 'mode', 'timestamp'] # Example column names
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    if 'mode' in df.columns:
                        df['mode'] = df['mode'].astype(str).str.lower().str.strip()
                    all_multimodal_dfs.append(df)
                else:
                    print(f"⚠️ Skipping {blob.name} due to insufficient columns.")
    if all_multimodal_dfs:
        return pd.concat(all_multimodal_dfs, ignore_index=True)
    else:
        print("No valid multimodal data found for preprocessing.")
        return None

def merge_and_engineer_features(mta_df, multimodal_df):
    """Merges MTA and multimodal data and engineers features."""
    if mta_df is None or multimodal_df is None:
        print("Cannot merge data as one of the DataFrames is missing.")
        return None

    # **This merge logic will heavily depend on how you can relate
    # records in the MTA data to records in the multimodal data.**
    # This is a placeholder and likely needs significant adjustment.
    print("⚠️ Warning: Merge and feature engineering logic needs to be implemented based on your data relationship.")
    # Example placeholder merge (very likely incorrect for your actual data):
    merged_df = pd.merge(mta_df, multimodal_df, left_index=True, right_index=True, how='inner')

    if 'DATETIME' in merged_df.columns:
        merged_df['hour'] = merged_df['DATETIME'].dt.hour
        merged_df['day_of_week'] = merged_df['DATETIME'].dt.dayofweek # Monday=0, Sunday=6
        merged_df['month'] = merged_df['DATETIME'].dt.month

    return merged_df

def save_processed_data_to_gcs(df, bucket_name, blob_name):
    """Saves the processed DataFrame directly to Google Cloud Storage as CSV."""
    if df is not None:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        blob.upload_from_string(buffer.getvalue(), content_type='text/csv')
        print(f"✅ Saved processed data to gs://{bucket_name}/{blob_name}")
    else:
        print(f"No DataFrame to save to GCS for {blob_name}.")

if __name__ == "__main__":
    # Read MTA data from GCS
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    mta_blobs = bucket.list_blobs(prefix=gcs_mta_prefix)
    mta_dataframes = []
    for blob in mta_blobs:
        if blob.name.endswith('.csv'):
            df = read_csv_from_gcs(gcs_bucket_name, blob.name)
            if df is not None:
                mta_dataframes.append(df)

    # Preprocess MTA data
    processed_mta_df = preprocess_mta_data(mta_dataframes)

    # Preprocess multimodal data from GCS (all TXT files in the archive)
    processed_multimodal_df = preprocess_multimodal_data(gcs_bucket_name, gcs_multimodal_prefix)

    # Merge and engineer features
    if processed_mta_df is not None and processed_multimodal_df is not None:
        final_df = merge_and_engineer_features(processed_mta_df, processed_multimodal_df)
        save_processed_data_to_gcs(final_df, gcs_bucket_name, gcs_processed_blob_name)
    else:
        print("Skipping merge and feature engineering due to missing data.")

    if processed_mta_df is not None:
        save_processed_data_to_gcs(processed_mta_df, gcs_bucket_name, 'processed/processed_mta_data.csv')

    if processed_multimodal_df is not None:
        save_processed_data_to_gcs(processed_multimodal_df, gcs_bucket_name, 'processed/processed_multimodal_data.csv')