
from kfp.v2 import dsl
from kfp.v2.dsl import component, Input, Output, Dataset, Model, Metrics
from google.cloud import aiplatform
import pandas as pd
import joblib
import json
import os

aiplatform.init(project="gps-ax-lakehouse", location="us-central1")

@component(
    packages_to_install=["pandas", "gcsfs"]
)
def load_managed_dataset(output_data: Output[Dataset]):
    df = pd.read_csv("gs://mta-ridership-data/inputs/merged_feature_data.csv")
    df.to_csv(output_data.path, index=False)

@component(
    packages_to_install=["pandas", "scikit-learn", "joblib", "google-cloud-storage"]
)
def train_model(data: Input[Dataset], model: Output[Model], metrics: Output[Metrics]):
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_squared_error
    from google.cloud import storage

    df = pd.read_csv(data.path)
    X = df[['weather_encoded', 'duration', 'fare', 'fare_per_minute']]
    y = df["ridership"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model_obj = RandomForestRegressor(n_estimators=100, random_state=42)
    model_obj.fit(X_train, y_train)

    local_model_path = model.path + ".pkl"
    joblib.dump(model_obj, local_model_path)

    storage.Client(project="gps-ax-lakehouse").bucket("mta-ridership-data").blob("models/ridership_model.pkl").upload_from_filename(local_model_path)

    predictions = model_obj.predict(X_test)
    rmse = mean_squared_error(y_test, predictions, squared=False)
    metrics.log_metric("rmse", rmse)

@dsl.pipeline(
    name="ridership-mode-choice-pipeline",
    description="Vertex AI pipeline to train and evaluate ridership model with managed dataset"
)
def ridership_pipeline():
    dataset_task = load_managed_dataset()
    model_task = train_model(data=dataset_task.outputs["output_data"])

from kfp.v2 import compiler
compiler.Compiler().compile(
    pipeline_func=ridership_pipeline,
    package_path="vertex_ridership_pipeline.json"
)
