import pandas as pd
from google.cloud import storage
import io
import os
from datetime import datetime

# Google Cloud Storage details
gcs_bucket_name = 'mta-ridership-data'  # Replace with your GCS bucket name
gcs_mta_prefix = 'raw/mta/'          # Prefix for MTA data
gcs_multimodal_prefix = 'raw/multimodal/' # Prefix for multimodal data
gcs_processed_blob_name = 'processed/merged_transport_data.csv' # Blob name for final processed data

def read_data_from_gcs(bucket_name, prefix):
    """Reads all CSV files from a given GCS prefix into a list of DataFrames."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    dfs = []
    for blob in blobs:
        if blob.name.endswith('.csv'):
            print(f"Reading data from gs://{bucket_name}/{blob.name}")
            try:
                content = blob.download_as_bytes()
                df = pd.read_csv(io.BytesIO(content))
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {blob.name}: {e}")
    return dfs

def read_multimodal_data_from_gcs(bucket_name, prefix):
    """Reads multimodal data (assuming it might be in a single .txt file) from GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    dfs = []
    for blob in blobs:
        if blob.name.endswith('.txt'):
            print(f"Reading multimodal data from gs://{bucket_name}/{blob.name}")
            try:
                content = blob.download_as_bytes()
                # Adjust the reading based on the actual structure of your .txt file
                # This is a placeholder - you'll need to inspect your .txt file
                df = pd.read_csv(io.BytesIO(content), sep='\t', header=None) # Example: tab-separated, no header
                dfs.append(df)
            except Exception as e:
                print(f"Error reading multimodal {blob.name}: {e}")
        elif blob.name.endswith('.csv'): # In case XLSX was converted
            print(f"Reading multimodal data from gs://{bucket_name}/{blob.name}")
            try:
                content = blob.download_as_bytes()
                df = pd.read_csv(io.BytesIO(content))
                dfs.append(df)
            except Exception as e:
                print(f"Error reading multimodal {blob.name}: {e}")
    return dfs

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

def preprocess_multimodal_data(multimodal_dfs):
    """Preprocesses the list of multimodal DataFrames."""
    if not multimodal_dfs:
        print("No multimodal data to preprocess.")
        return None
    multimodal_df = pd.concat(multimodal_dfs, ignore_index=True)
    # **You will need to adapt this part based on the actual structure
    # of your multimodal .txt or .csv file.**
    print("⚠️ Warning: Multimodal data preprocessing needs to be implemented based on the actual file structure.")
    # Example placeholders:
    if multimodal_df.shape[1] >= 3:
        multimodal_df.columns = ['user_id', 'mode', 'timestamp'] # Example column names
        if 'timestamp' in multimodal_df.columns:
            multimodal_df['timestamp'] = pd.to_datetime(multimodal_df['timestamp'], errors='coerce')
        if 'mode' in multimodal_df.columns:
            multimodal_df['mode'] = multimodal_df['mode'].astype(str).str.lower().str.strip()
    return multimodal_df

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
    # Read data from GCS
    mta_dataframes = read_data_from_gcs(gcs_bucket_name, gcs_mta_prefix)
    multimodal_dataframes = read_multimodal_data_from_gcs(gcs_bucket_name, gcs_multimodal_prefix)

    # Preprocess data
    processed_mta_df = preprocess_mta_data(mta_dataframes)
    processed_multimodal_df = preprocess_multimodal_data(multimodal_dataframes)

    # Merge and engineer features
    if processed_mta_df is not None and processed_multimodal_df is not None:
        final_df = merge_and_engineer_features(processed_mta_df, processed_multimodal_df)
        save_processed_data_to_gcs(final_df, gcs_bucket_name, gcs_processed_blob_name)
    else:
        print("Skipping merge and feature engineering due to missing data.")

    # You can optionally save the preprocessed individual DataFrames to GCS as well
    # if needed for other steps.
    # save_processed_data_to_gcs(processed_mta_df, gcs_bucket_name, 'processed/processed_mta_data.csv')
    # save_processed_data_to_gcs(processed_multimodal_df, gcs_bucket_name, 'processed/processed_multimodal_data.csv')