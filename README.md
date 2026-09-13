# GCP Employee ETL Data Pipeline

An end-to-end data engineering project built on Google Cloud Platform (GCP) to process employee data using a cloud-based ETL pipeline.

The project uses Google Cloud Storage to store raw and processed data, Dataproc with PySpark for data transformation, BigQuery for analytical storage, and Cloud Composer (Managed Airflow) for workflow orchestration.

## Architecture

CSV File → Google Cloud Storage → Airflow → Dataproc (PySpark) → GCS Processed Data → BigQuery → Validation

## Technologies Used

- Google Cloud Storage (GCS)
- Google Cloud Dataproc
- PySpark
- Google BigQuery
- Cloud Composer / Managed Apache Airflow
- Python
- SQL
- Airflow XCom
- Airflow Sensors & Operators

## Project Workflow

1. Employee CSV data is uploaded to the raw GCS bucket.
2. Airflow checks whether the input file exists using a GCS sensor.
3. Airflow uses XCom to pass the input file path.
4. Airflow triggers a PySpark ETL job on Dataproc.
5. PySpark transforms the employee data:
   - Converts department and city values to uppercase.
   - Calculates a 10% employee bonus.
   - Adds a processing timestamp.
6. The processed data is written to GCS in Parquet format.
7. Airflow loads the processed data into BigQuery.
8. A BigQuery validation query calculates employee count and average salary by department.

## Final Result

The pipeline successfully processed 10 employee records and loaded the transformed data into BigQuery.

The final validation confirmed the expected department-wise employee count and average salary.

This project demonstrates an end-to-end cloud data engineering workflow with data storage, transformation, orchestration, data loading, and validation.
