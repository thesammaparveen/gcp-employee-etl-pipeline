from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from datetime import timedelta


PROJECT_ID = "complete-will-508311-t3"
REGION = "us-central1"
CLUSTER_NAME = "demo-cluster"

BQ_DATASET = "demo_dataset"
BQ_TABLE = "employee_processed"

RAW_BUCKET = "april-raw-data-bucket-samma"
PROCESSED_BUCKET = "april-processed-data-bucket-samma"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="gcp_employee_etl_pipeline",
    default_args=default_args,
    description="End to End GCP ETL Pipeline using GCS Dataproc BigQuery Composer",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gcp", "dataproc", "bigquery", "composer"]
) as dag:

    # Task 1: Check File Exists in GCS
    check_gcs_file = GCSObjectExistenceSensor(
        task_id="check_gcs_file",
        bucket=RAW_BUCKET,
        object="input/employee_data.csv",
        timeout=60,
        poke_interval=10,
        mode="poke"
    )


    # Task 2: Push Input Path to XCom
    def push_input_path(**context):
        input_file = f"gs://{RAW_BUCKET}/input/employee_data.csv"

        context["ti"].xcom_push(
            key="input_file_path",
            value=input_file
        )

        print(f"Pushed Input File Path: {input_file}")


    push_xcom_task = PythonOperator(
        task_id="push_input_path",
        python_callable=push_input_path
    )


    # Task 3: Dataproc PySpark Job
    PYSPARK_JOB = {
        "reference": {
            "project_id": PROJECT_ID
        },
        "placement": {
            "cluster_name": CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri":
                f"gs://{RAW_BUCKET}/scripts/employee_etl.py"
        }
    }


    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_dataproc_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )


    # Task 4: Load Data from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=PROCESSED_BUCKET,
        source_objects=[
            "output/employee_data_processed/*.parquet"
        ],
        destination_project_dataset_table=
            f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )


    # Task 5: Run BigQuery Validation Query
    validation_query = BigQueryInsertJobOperator(
        task_id="bigquery_validation_query",
        configuration={
            "query": {
                "query": f"""
                    SELECT
                        department,
                        COUNT(*) AS employee_count,
                        AVG(salary) AS avg_salary
                    FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
                    GROUP BY department
                    ORDER BY avg_salary DESC
                """,
                "useLegacySql": False
            }
        }
    )


    # Task 6: Pull XCom Example
    def read_xcom(**context):
        file_path = context["ti"].xcom_pull(
            task_ids="push_input_path",
            key="input_file_path"
        )

        print(f"Received from XCom: {file_path}")


    read_xcom_task = PythonOperator(
        task_id="read_xcom_task",
        python_callable=read_xcom
    )


    # DAG Flow
    check_gcs_file >> push_xcom_task >> read_xcom_task
    read_xcom_task >> run_dataproc_job
    run_dataproc_job >> load_to_bigquery >> validation_query