# GCP Employee ETL Data Pipeline

An end-to-end data engineering project built on Google Cloud Platform (GCP) to process employee data using a cloud-based ETL pipeline.

The project uses Google Cloud Storage (GCS) for data storage, Dataproc with PySpark for data transformation, BigQuery for analytical storage, and Cloud Composer (Managed Service for Apache Airflow) for workflow orchestration.

---

## Project Objective

The objective of this project is to build an end-to-end cloud data pipeline that:

- Stores raw employee data in Google Cloud Storage
- Validates the input file using Airflow
- Processes and transforms the data using PySpark on Dataproc
- Stores the processed data in GCS in Parquet format
- Loads the processed data into BigQuery
- Performs validation using a BigQuery SQL query
- Demonstrates Airflow XCom, sensors, operators, and task dependencies

---

## Architecture

```text
CSV File
   |
   v
Google Cloud Storage
(Raw Data)
   |
   v
Cloud Composer / Airflow
   |
   +--> Check GCS File
   |
   +--> XCom - Pass Input Path
   |
   v
Dataproc
(PySpark ETL)
   |
   v
Google Cloud Storage
(Processed Parquet Data)
   |
   v
BigQuery
(employee_processed)
   |
   v
Validation Query
