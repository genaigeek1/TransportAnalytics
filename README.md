
# 🚍 TransportAnalytics

A complete end-to-end machine learning project built on **Google Vertex AI** to forecast **transit ridership revenue (ROI)** and predict **mode of transportation** based on factors such as fare, duration, and weather. This repository includes data ingestion, feature engineering, model training, and deployment pipeline orchestration.

---

## 📁 Repository Structure

```
TransportAnalytics/
├── config/                    # Configuration files for pipeline, training, or GCP integration
│   └── .placeholder
├── data_ingestion/           # Scripts to fetch and upload datasets to Google Cloud
│   └── download_kaggle_and_upload_gcs.py
├── deployment/               # (Optional) Scripts for batch or real-time model predictions
│   └── .placeholder
├── notebooks/                # Jupyter notebooks for EDA and insights
│   ├── eda_mta_ridership.ipynb
│   └── eda_mode_choice.ipynb
├── pipeline/                 # Vertex AI pipeline orchestration scripts
│   └── vertex_pipeline.py
├── preprocessing/            # Feature engineering scripts and processed datasets
│   ├── feature_engineering.py
│   └── merged_feature_data.csv
├── training/                 # Model training scripts
│   ├── train_ridership_model.py
│   └── train_mode_classifier.py
├── .gitignore
└── README.md
```

---

## 🚀 Running the Pipeline on Google Vertex AI

### ✅ Prerequisites

- Google Cloud Project (`your-gcp-projectid`)
- Vertex AI API enabled
- BigQuery and Cloud Storage set up
- Service Account with Vertex AI permissions
- Python ≥ 3.8

### 🔧 Step-by-Step Setup

1. **Clone the repository:**
```bash
git clone https://github.com/YOUR_USERNAME/TransportAnalytics.git
cd TransportAnalytics
```

2. **Create and activate a virtual environment (required in Cloud Shell or locally):**
> This ensures isolated package installations and avoids permission issues, especially in Google Cloud Shell.

```bash
python3 -m venv venv
source venv/bin/activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
# or manually:
pip install google-cloud-aiplatform kfp pandas scikit-learn
```

4. **Download datasets from Kaggle and upload to GCS (required before feature engineering):**
```bash
export KAGGLE_USERNAME=you_kaggle_username
export KAGGLE_KEY=your_kaggle_key
python data_ingestion/download_kaggle_and_upload_gcs.py
```
**Note:** This step is mandatory before running the feature engineering script.

5. **Run feature engineering:**
```bash
python preprocessing/feature_engineering.py
```

6. **Train models locally (optional):**
```bash
python training/train_ridership_model.py
python training/train_mode_classifier.py
```

7. **Compile and submit Vertex AI pipeline:**
```bash
python pipeline/vertex_pipeline.py
```

8. **Deploy the pipeline using Python SDK:**
```python
from google.cloud import aiplatform
from google.cloud.aiplatform.pipeline_jobs import PipelineJob

aiplatform.init(project="your-gcp-projectid", location="your-gcp-project-location")

pipeline_job = PipelineJob(
    display_name="ridership-forecast-pipeline",
    template_path="vertex_ridership_pipeline.json",
    enable_caching=True,
)
pipeline_job.run()
```

---

## 🧠 Project Highlights

- **Datasets Used:**
  - [MTA Ridership](https://www.kaggle.com/datasets/princehobby/metropolitan-transportation-authority-mta-datasets)
  - [Multimodal Mode Choice](https://www.kaggle.com/datasets/merdelic/dataset-for-multimodal-transport-analytics)

- **ML Models:**
  - `RandomForestRegressor`: Predict transit ridership revenue
  - `RandomForestClassifier`: Predict mode of transport based on influencing factors

- **Feature Engineering Highlights:**
  - Encoding categorical variables like `weather` and `mode`
  - Engineering features like `fare_per_minute`
  - Merging datasets on common temporal dimensions

---

## 🧩 Coming Soon
- Cloud function or batch endpoint for scoring
- BigQuery ML support
- Looker Studio dashboard for visualization

---

## 📬 Feedback & Contributions
Feel free to fork this repo, submit pull requests, or create issues. Contributions are welcome!

