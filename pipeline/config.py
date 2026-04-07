import os

PROJECT_ID = os.environ.get("PROJECT_ID", "enter_project_id")
REGION = os.environ.get("REGION", "us-central1")
INPUT_BUCKET = os.environ.get("INPUT_BUCKET", "gs://raw_data_hackaton/data/fr_data_uservoice.csv") #bucket where the CSV files will be uploaded 
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "gs://pipeline_bucket_hackaton/embeddings/real_data")#The bucket for embeddings output, staging and temp
STAGING_BUCKET = os.environ.get("STAGING_BUCKET", "gs://pipeline_bucket_hackaton/staging/real_data")
TEMP_BUCKET = os.environ.get("TEMP_BUCKET", "gs://pipeline_bucket_hackaton/temp/real_data")
BQ_TABLE = f"{PROJECT_ID}:etl_dataset.cleaned_records_real_data"
EMBEDDING_MODEL = "text-embedding-005"
CSV_HEADERS = ["title", "description", "url", "extracted_at"]

