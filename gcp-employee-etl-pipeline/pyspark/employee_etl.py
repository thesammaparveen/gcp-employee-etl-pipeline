from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

spark = SparkSession.builder \
    .appName("EmployeeETL") \
    .getOrCreate()

input_path = "gs://april-raw-data-bucket-samma/input/employee_data.csv"
output_path = "gs://april-processed-data-bucket-samma/output/employee_data_processed"

employee_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)

print("===== RAW DATA =====")
employee_df.show()

transformed_df = employee_df \
    .withColumn("department", upper(col("department"))) \
    .withColumn("city", upper(col("city"))) \
    .withColumn("bonus", col("salary") * 0.10) \
    .withColumn("processing_timestamp", current_timestamp())

print("===== TRANSFORMED DATA =====")
transformed_df.show()

transformed_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .parquet(output_path)

print("===== DATA WRITTEN SUCCESSFULLY =====")

spark.stop()