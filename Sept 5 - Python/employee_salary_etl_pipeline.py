# employee_salary_etl_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Employee Salary ETL Pipeline") \
    .getOrCreate()

# Load employee data from CSV file
# Assuming the file is named 'employee_data.csv'
file_path = '/content/sample_data/employee_data.csv'
employee_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the loaded data
employee_df.show()

# Filter employees aged 30 and above
filtered_df = employee_df.filter(employee_df.age >= 30)

# Show filtered data
filtered_df.show()

# Calculate the salary with a 10% bonus
transformed_df = filtered_df.withColumn("salary_with_bonus", filtered_df.salary * 1.10)

# Show the transformed data with the bonus
transformed_df.show()

# Group by gender and calculate the average salary
avg_salary_by_gender = transformed_df.groupBy("gender").agg(F.avg("salary").alias("average_salary"))

# Show the average salary by gender
avg_salary_by_gender.show()

# Save the transformed data to a Parquet file
parquet_output_path = 'transformed_employee_data.parquet'
transformed_df.write.parquet(parquet_output_path, mode='overwrite')

print("Transformed data saved to Parquet format at:", parquet_output_path)

# Stop the Spark session
spark.stop()
