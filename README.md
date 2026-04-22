**Hackaton Beam ETL Pipeline**

Description: An ETL pipeline built with Apache Beam and GCP, designed to process user voice data and load it into BigQuery and generate embeddings using Vertex AI.

Overview:

This Apache Beam batch ETL pipeline perfoms the following:
* Read CSV files from a Google Cloud Storage bucket.
* Perform cleaning and validation.
* Generate embeddings.
* Upload embeddings to a Google Cloud Storage bucket.
* Load cleaned and validated data to a BigQuery table.

Data: The sample dataset `fr_data_uservoice.csv` contains user voice records (https://firebase.uservoice.com/forums/948424-general) used as input for the pipeline.

Clone the repo:
```
git clone https://github.com/AlexCC1901/hackaton-beam-etl-pipeline.git`
cd hackaton-beam-etl-pipeline
```
Create and activate a virtual environment:
```
python -m venv venv
venv\Scripts\activate
```
Install dependencies:
```
pip install -r requirements.txt
```

Run locally (DirectRunner):
```
python pipeline/main.py \
--runner Directrunner \
--input fr_data_uservoice.csv \
--project YOUR_GCP_PROJECT_ID \
--output YOUR_BIGQUERY_DATASET.YOUR_TABLE
```

Author: AlexCC1901
