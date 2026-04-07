import multiprocessing
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, StandardOptions
)
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from google.cloud import storage
import pandas as pd
import io

from pipeline.config import (
    PROJECT_ID, REGION, INPUT_BUCKET, OUTPUT_BUCKET,
    STAGING_BUCKET, TEMP_BUCKET, BQ_TABLE,
    EMBEDDING_MODEL, CSV_HEADERS,
)
from pipeline.transforms.cleaning import CleanAndValidate, to_bq_row
from pipeline.transforms.embeddings import GenerateEmbedding, FormatAsJSONL

BQ_SCHEMA = {
    "fields": [
        {"name": "id",           "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "title",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "description",  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "url",          "type": "STRING",    "mode": "NULLABLE"},
        {"name": "extracted_at", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]
}


def read_csv_from_gcs(bucket_name, blob_path):

    client = storage.Client() #create GCS client
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path) #point to the file inside the folder
    content = blob.download_as_text(encoding="utf-8") #downloads the entire CSV file as a text string into memory
    df = pd.read_csv(io.StringIO(content)) #io.StringIO(content) converts the string into a csv file. pd.read_csv converts the file into a pandas dataframe.
    df = df.fillna("")  # replace missing cells (NaN) with empty strings ("").
    return df.to_dict(orient="records") #converts the dataframe into a list of dictionaries. One dictionary per row in the dataframe. e.g. {"title": "Proposal...", "description": "I recommend...", "url": "https://...", "extracted_at": "2026-03-23"},


def build_pipeline_options(runner="DataflowRunner"): #default runner
    options = PipelineOptions()

    google_opts = options.view_as(GoogleCloudOptions)
    google_opts.project          = PROJECT_ID
    google_opts.region           = REGION
    google_opts.staging_location = STAGING_BUCKET
    google_opts.temp_location    = TEMP_BUCKET
    google_opts.job_name         = "etl-csv-to-bq-embeddings"

    std_opts = options.view_as(StandardOptions)
    std_opts.runner = runner

    return options


def run(runner="DataflowRunner"):
    options = build_pipeline_options(runner)

    # Read CSV using pandas (handles multiline fields correctly)
    print("Reading CSV from GCS...")
    records = read_csv_from_gcs(
        bucket_name="raw_data_hackaton",
        blob_path="data/fr_data_uservoice.csv"
    )

    #enumerate iterates on the list of dictionaries "records". for each element in the list (each dictionary) create a new dictionary with the shape {"id": i + 1, **record}. i is the index of the dictionary on the original list (starts with 0 so we sum 1), and  **record takes the element "record" (which is a dictionary of the original list) and takes all its key-value pairs and add it to the new dictionary. [] indicates this is a list of dictionaries. 
    #This is a list comprehension
    records = [{"id": i + 1, **record} for i, record in enumerate(records)]

    print(f"Read {len(records)} records from CSV")

    with beam.Pipeline(options=options) as p:

        #Create PCollection from records
        cleaned = (
            p
            | "Create Records"   >> beam.Create(records) #this PTransform creates a PCollection from in'memory data. It takes the list "records" and injects it into the pipeline as a PCollection.
            | "Clean & Validate" >> beam.ParDo(CleanAndValidate())
            | "Reshuffle"        >> beam.Reshuffle()
        )

        #Write to BigQuery 
        (
            cleaned
            | "To BQ Row"      >> beam.Map(to_bq_row)
            | "Write BigQuery" >> WriteToBigQuery(
                BQ_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        #Generate Embeddings and Write to GCS
        (
            cleaned
            | "Generate Embeddings" >> beam.ParDo(
                GenerateEmbedding(PROJECT_ID, REGION, EMBEDDING_MODEL)
              )
            | "Format as JSONL"     >> beam.ParDo(FormatAsJSONL())
            | "Write Embeddings"    >> WriteToText( #takes all elements in the PCollection and saves them as a .jsonl file in the specified bucket. 
                f"{OUTPUT_BUCKET}/embeddings",
                file_name_suffix=".jsonl", #A jsonl file is a file weher each line is a JSON object
              )
        )


if __name__ == "__main__":
    multiprocessing.freeze_support()
    run(runner="DirectRunner") #using direct runner to test locally.