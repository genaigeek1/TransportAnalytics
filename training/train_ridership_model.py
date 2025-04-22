
import pandas as pd
import json
import joblib
from google.cloud import storage
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# Load config
with open('config/feature_list.json') as f:
    features_config = json.load(f)
with open('config/gcp_config.json') as f:
    gcp_config = json.load(f)

features = features_config["common_features"]
target = features_config["target_ridership"]

# Download dataset from GCS
client = storage.Client(project=gcp_config["project_id"])
bucket = client.bucket(gcp_config["bucket"])
blob = bucket.blob(gcp_config["gcs_input_path"])
local_path = "/tmp/merged_feature_data.csv"
blob.download_to_filename(local_path)

df = pd.read_csv(local_path)
X = df[features]
y = df[target]

# Train model
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate
predictions = model.predict(X_test)
rmse = mean_squared_error(y_test, predictions, squared=False)
print(f'RMSE on test set: {rmse:.2f}')

# Save and upload model
local_model_path = "/tmp/ridership_model.pkl"
joblib.dump(model, local_model_path)
bucket.blob(gcp_config["gcs_model_path"]).upload_from_filename(local_model_path)
print(f"Model uploaded to: gs://{gcp_config['bucket']}/{gcp_config['gcs_model_path']}")
