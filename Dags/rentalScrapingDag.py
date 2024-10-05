from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import json
import tempfile
import re

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Function to clean price and area fields
def clean_data(rental_data):
    cleaned_data = []

    for rental in rental_data:
        if "EUR" in rental["price"] or "Prix à consulter" in rental["price"]:
            continue

        price_str = rental["price"].replace("DH", "").replace(" ", "").strip()
        price = float(price_str)

        # Extract the area in m²
        area_match = re.search(r"(\d+)\s*m²", rental["area"])
        area = float(area_match.group(1)) if area_match else None

        cleaned_rental = {
            "location": rental["location"],
            "price": price,
            "area": area,
            "date": rental["Date"],  # Keeping the date as is for now
        }
        cleaned_data.append(cleaned_rental)

    return cleaned_data


dag = DAG(
    "cloud_run_to_gcs_bigquery_with_bucket_creation_dag",
    default_args=default_args,
    description="A DAG that fetches data from Cloud Run, stores it in a new GCS bucket, and loads it to BigQuery",
    schedule_interval=timedelta(days=1),
)


BUCKET_NAME = "scraperentals"
GCS_OBJECT_NAME = "rentals_data.ndjson"
PROJECT_ID = "scientific-elf-437313-q7"

get_json_from_cloud_run = SimpleHttpOperator(
    task_id="get_json_from_cloud_run",
    method="GET",
    http_conn_id="cloud_run_service",
    endpoint="/",
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    dag=dag,
)


def upload_json_to_gcs(ti):
    json_data = ti.xcom_pull(task_ids="get_json_from_cloud_run")

    cleaned_data = clean_data(json_data)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".ndjson", delete=False
    ) as temp_file:
        for item in cleaned_data:
            temp_file.write(json.dumps(item) + "\n")
        temp_file.flush()
        temp_file_name = temp_file.name

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_OBJECT_NAME)

    blob.upload_from_filename(temp_file_name)

    return f"Uploaded {temp_file_name} to GCS as {GCS_OBJECT_NAME}"


upload_json_task = PythonOperator(
    task_id="upload_json_to_gcs",
    python_callable=upload_json_to_gcs,
    dag=dag,
)

DATASET_NAME = "rentals"
STAGING_TABLE = "rentals_staging"
FINAL_TABLE = "rentals_transformed"

load_gcs_to_bigquery = GCSToBigQueryOperator(
    task_id="load_gcs_to_bigquery",
    bucket=BUCKET_NAME,
    source_objects=[GCS_OBJECT_NAME],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{STAGING_TABLE}",
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    dag=dag,
)

sql_transformations = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME}.{FINAL_TABLE}` AS
WITH location_data AS (
    SELECT 
        location, 
        AVG(price) AS avg_price, 
        MAX(price) AS max_price, 
        MIN(price) AS min_price, 
        APPROX_TOP_COUNT(area, 1)[OFFSET(0)].value AS most_common_area
    FROM `{PROJECT_ID}.{DATASET_NAME}.{STAGING_TABLE}`
    GROUP BY location
)
SELECT * FROM location_data;
"""

transform_data_in_bigquery = BigQueryInsertJobOperator(
    task_id="transform_data_in_bigquery",
    configuration={
        "query": {
            "query": sql_transformations,
            "useLegacySql": False,
        }
    },
    dag=dag,
)

(
    get_json_from_cloud_run
    >> upload_json_task
    >> load_gcs_to_bigquery
    >> transform_data_in_bigquery
)
