### Task 1: Raw Data Ingestion
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("Weather Data Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema for the weather data
weather_schema = StructType([
    StructField("City", StringType(), True),
    StructField("Date", DateType(), True),
    StructField("Temperature", FloatType(), True),
    StructField("Humidity", IntegerType(), True)
])

# Define file path for the raw data CSV
file_path = "/path/to/weather_data.csv"

# Check if the file exists
if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
    # Log the error
    with open("/path/to/logs/ingestion_logs.txt", "a") as log_file:
        log_file.write(f"Error: Weather data file {file_path} does not exist\n")
else:
    # Proceed to load and process the data
    print(f"File found: {file_path}")

    # Load the CSV data with the defined schema
    raw_weather_data = spark.read.csv(file_path, header=True, schema=weather_schema)

    # Show a few rows to verify
    raw_weather_data.show(5)

    # Define Delta table path
    delta_table_path = "/path/to/delta/weather_data"

    # Write data to Delta table
    raw_weather_data.write.format("delta").mode("overwrite").save(delta_table_path)

    print(f"Raw data successfully saved to Delta table at {delta_table_path}")
```

### Task 2: Data Cleaning
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Weather Data Cleaning") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the path to the Delta table
delta_table_path = "/path/to/delta/weather_data"
# Load the raw data from the Delta table
raw_weather_data = spark.read.format("delta").load(delta_table_path)
# Show the raw data
raw_weather_data.show(5)
# Remove rows with missing or null values
cleaned_weather_data = raw_weather_data.dropna()
# Show the cleaned data
cleaned_weather_data.show(5)
# Define path for the cleaned Delta table
cleaned_delta_table_path = "/path/to/delta/cleaned_weather_data"
# Save the cleaned data to a new Delta table
cleaned_weather_data.write.format("delta").mode("overwrite").save(cleaned_delta_table_path)
print(f"Cleaned data successfully saved to Delta table at {cleaned_delta_table_path}")
```

### Task 3: Data Transformation
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Weather Data Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define the path to the cleaned Delta table
cleaned_delta_table_path = "/path/to/delta/cleaned_weather_data"
# Load the cleaned data from the Delta table
cleaned_weather_data = spark.read.format("delta").load(cleaned_delta_table_path)
# Show the cleaned data
cleaned_weather_data.show(5)
# Calculate average temperature and humidity for each city
transformed_data = cleaned_weather_data.groupBy("City").agg(
    F.avg("Temperature").alias("Average_Temperature"),
    F.avg("Humidity").alias("Average_Humidity")
)
# Show the transformed data
transformed_data.show()
# Define path for the transformed Delta table
transformed_delta_table_path = "/path/to/delta/transformed_weather_data"
# Save the transformed data to a new Delta table
transformed_data.write.format("delta").mode("overwrite").save(transformed_delta_table_path)

print(f"Transformed data successfully saved to Delta table at {transformed_delta_table_path}")
```

### Task 4: Build and Run a Pipeline 
```python
import subprocess
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define paths to the notebooks
notebooks = {
    "Raw Data Ingestion": "/path/to/Raw_Data_Ingestion_Notebook.ipynb",
    "Data Cleaning": "/path/to/Data_Cleaning_Notebook.ipynb",
    "Data Transformation": "/path/to/Data_Transformation_Notebook.ipynb"
}

# Function to execute a notebook
def execute_notebook(notebook_path):
    try:
        # Execute the notebook using a command-line tool (e.g., nbconvert or databricks-cli)
        result = subprocess.run(['databricks', 'notebook', 'run', '--path', notebook_path], check=True)
        logging.info(f"Successfully executed {notebook_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to execute {notebook_path}: {e}")
        return False

# Main pipeline execution
def run_pipeline():
    for name, path in notebooks.items():
        # Check if the notebook file exists
        if not os.path.exists(path):
            logging.error(f"Notebook file not found: {path}")
            break

        # Execute the notebook
        success = execute_notebook(path)
        if not success:
            logging.error(f"Pipeline failed at step: {name}")
            break
    else:
        logging.info("Pipeline executed successfully!")

if __name__ == "__main__":
    run_pipeline()
```

---

### Task 1: Customer Data Ingestion
```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("CustomerDataIngestion").getOrCreate()

# Path to the CSV file
file_path = "/path/to/customer_transactions.csv"

try:
    # Load CSV file into a DataFrame
    customer_df = spark.read.option("header", True).csv(file_path)

    # Write DataFrame into a Delta table
    customer_df.write.format("delta").mode("overwrite").save("/delta/customer_transactions")
except AnalysisException as e:
    print(f"File not found or error while loading the file: {e}")
```

### Task 2: Data Cleaning
```python
# Drop duplicates
cleaned_df = customer_df.dropDuplicates()

# Handle nulls in the TransactionAmount column by filling with 0
cleaned_df = cleaned_df.fillna({'TransactionAmount': 0})

# Write cleaned data to a new Delta table
cleaned_df.write.format("delta").mode("overwrite").save("/delta/cleaned_customer_transactions")
```

### Task 3: Data Aggregation
```python
# Aggregate data
aggregated_df = cleaned_df.groupBy("ProductCategory").sum("TransactionAmount").alias("TotalTransactionAmount")

# Save aggregated data to a Delta table
aggregated_df.write.format("delta").mode("overwrite").save("/delta/aggregated_customer_transactions")
```

### Task 4: Pipeline Creation
```python
def ingest_data(file_path):
    try:
        customer_df = spark.read.option("header", True).csv(file_path)
        customer_df.write.format("delta").mode("overwrite").save("/delta/customer_transactions")
        return customer_df
    except Exception as e:
        print(f"Error during ingestion: {e}")
        return None

def clean_data(df):
    try:
        df_cleaned = df.dropDuplicates().fillna({'TransactionAmount': 0})
        df_cleaned.write.format("delta").mode("overwrite").save("/delta/cleaned_customer_transactions")
        return df_cleaned
    except Exception as e:
        print(f"Error during cleaning: {e}")
        return None

def aggregate_data(df_cleaned):
    try:
        df_aggregated = df_cleaned.groupBy("ProductCategory").sum("TransactionAmount")
        df_aggregated.write.format("delta").mode("overwrite").save("/delta/aggregated_customer_transactions")
    except Exception as e:
        print(f"Error during aggregation: {e}")

# File path to the raw data
file_path = "/path/to/customer_transactions.csv"

# Execute the pipeline
df_raw = ingest_data(file_path)
if df_raw is not None:
    df_cleaned = clean_data(df_raw)
    if df_cleaned is not None:
        aggregate_data(df_cleaned)
```

### Task 5: Data Validation
```python
# Get the total transaction count from raw data
total_transactions_raw = df_cleaned.count()

# Get the total transaction amount from the aggregated data
df_aggregated = spark.read.format("delta").load("/delta/aggregated_customer_transactions")
total_transactions_aggregated = df_aggregated.selectExpr("sum(TransactionAmount)").collect()[0][0]

if total_transactions_raw == total_transactions_aggregated:
    print("Data validation passed!")
else:
    print("Data validation failed!")
```

---

### Task 1: Product Inventory Data Ingestion
```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("ProductInventoryIngestion").getOrCreate()

# Path to the CSV file
file_path = "/path/to/product_inventory.csv"

try:
    # Load CSV into a DataFrame
    inventory_df = spark.read.option("header", True).csv(file_path)

    # Write DataFrame into a Delta table
    inventory_df.write.format("delta").mode("overwrite").save("/delta/product_inventory")

except AnalysisException as e:
    print(f"File not found or error loading file: {e}")
```

### Task 2: Data Cleaning
```python
# Remove rows with negative StockQuantity
cleaned_inventory_df = inventory_df.filter(inventory_df.StockQuantity >= 0)

# Fill null values in StockQuantity and Price columns
cleaned_inventory_df = cleaned_inventory_df.fillna({'StockQuantity': 0, 'Price': 0})

# Write cleaned data to a new Delta table
cleaned_inventory_df.write.format("delta").mode("overwrite").save("/delta/cleaned_product_inventory")
```

### Task 3: Inventory Analysis
```python
from pyspark.sql.functions import col

# Calculate total stock value
inventory_analysis_df = cleaned_inventory_df.withColumn("TotalStockValue",
    col("StockQuantity") * col("Price"))

# Find products that need restocking
restock_df = cleaned_inventory_df.filter(cleaned_inventory_df.StockQuantity < 100)

# Save analysis results to a Delta table
inventory_analysis_df.write.format("delta").mode("overwrite").save("/delta/inventory_analysis")
restock_df.write.format("delta").mode("overwrite").save("/delta/restock_products")
```

### Task 4: Build an Inventory Pipeline
```python
def ingest_product_data(file_path):
    try:
        inventory_df = spark.read.option("header", True).csv(file_path)
        inventory_df.write.format("delta").mode("overwrite").save("/delta/product_inventory")
        return inventory_df
    except Exception as e:
        print(f"Error during ingestion: {e}")
        return None

def clean_product_data(df):
    try:
        cleaned_df = df.filter(df.StockQuantity >= 0).fillna({'StockQuantity': 0, 'Price': 0})
        cleaned_df.write.format("delta").mode("overwrite").save("/delta/cleaned_product_inventory")
        return cleaned_df
    except Exception as e:
        print(f"Error during cleaning: {e}")
        return None

def analyze_inventory(df_cleaned):
    try:
        analysis_df = df_cleaned.withColumn("TotalStockValue", col("StockQuantity") * col("Price"))
        restock_df = df_cleaned.filter(df_cleaned.StockQuantity < 100)
        analysis_df.write.format("delta").mode("overwrite").save("/delta/inventory_analysis")
        restock_df.write.format("delta").mode("overwrite").save("/delta/restock_products")
    except Exception as e:
        print(f"Error during analysis: {e}")

# File path to the raw data
file_path = "/path/to/product_inventory.csv"

# Execute the pipeline
df_raw = ingest_product_data(file_path)
if df_raw is not None:
    df_cleaned = clean_product_data(df_raw)
    if df_cleaned is not None:
        analyze_inventory(df_cleaned)
```

### Task 5: Inventory Monitoring
```python
from pyspark.sql import DataFrame

# Load the cleaned product inventory Delta table
df_inventory = spark.read.format("delta").load("/delta/cleaned_product_inventory")

# Find products that need restocking
restock_alert_df = df_inventory.filter(df_inventory.StockQuantity < 50)

# Send an alert if any product needs restocking
if restock_alert_df.count() > 0:
    print("ALERT: Some products need restocking!")
else:
    print("All products are sufficiently stocked.")
```

---

### Task 1: Employee Attendance Data Ingestion
```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("EmployeeAttendanceIngestion").getOrCreate()

# Path to the CSV file
file_path = "/path/to/employee_attendance.csv"

try:
    # Load CSV into DataFrame
    attendance_df = spark.read.option("header", True).csv(file_path)

    # Write the DataFrame into a Delta table
    attendance_df.write.format("delta").mode("overwrite").save("/delta/employee_attendance")

except AnalysisException as e:
    print(f"File not found or error while loading the file: {e}")
```

### Task 2: Data Cleaning
```python
from pyspark.sql.functions import col, unix_timestamp

# Remove rows with null or invalid CheckInTime or CheckOutTime
cleaned_df = attendance_df.filter((col("CheckInTime").isNotNull()) &
                                   (col("CheckOutTime").isNotNull()))

# Calculate HoursWorked based on CheckInTime and CheckOutTime
cleaned_df = cleaned_df.withColumn("CalculatedHoursWorked",
    (unix_timestamp("CheckOutTime", 'HH:mm') - unix_timestamp("CheckInTime", 'HH:mm')) / 3600)

# Write cleaned data to a new Delta table
cleaned_df.write.format("delta").mode("overwrite").save("/delta/cleaned_employee_attendance")
```

### Task 3: Attendance Summary
```python
from pyspark.sql.functions import month, sum

# Calculate total hours worked by each employee for the current month
current_month = 3
summary_df = cleaned_df.filter(month(col("Date")) == current_month) \
    .groupBy("EmployeeID") \
    .agg(sum("CalculatedHoursWorked").alias("TotalHoursWorked"))

# Find employees who worked overtime (more than 8 hours on any given day)
overtime_df = cleaned_df.filter(col("CalculatedHoursWorked") > 8)

# Save the summary and overtime data to Delta tables
summary_df.write.format("delta").mode("overwrite").save("/delta/employee_attendance_summary")
overtime_df.write.format("delta").mode("overwrite").save("/delta/employee_overtime")
```

### Task 4: Create an Attendance Pipeline
```python
def ingest_attendance_data(file_path):
    try:
        attendance_df = spark.read.option("header", True).csv(file_path)
        attendance_df.write.format("delta").mode("overwrite").save("/delta/employee_attendance")
        return attendance_df
    except Exception as e:
        print(f"Error during ingestion: {e}")
        return None

def clean_attendance_data(df):
    try:
        cleaned_df = df.filter((col("CheckInTime").isNotNull()) &
                               (col("CheckOutTime").isNotNull()))
        cleaned_df = cleaned_df.withColumn("CalculatedHoursWorked",
            (unix_timestamp("CheckOutTime", 'HH:mm') - unix_timestamp("CheckInTime", 'HH:mm')) / 3600)
        cleaned_df.write.format("delta").mode("overwrite").save("/delta/cleaned_employee_attendance")
        return cleaned_df
    except Exception as e:
        print(f"Error during cleaning: {e}")
        return None

def summarize_attendance(df_cleaned):
    try:
        summary_df = df_cleaned.groupBy("EmployeeID").agg(sum("CalculatedHoursWorked").alias("TotalHoursWorked"))
        overtime_df = df_cleaned.filter(col("CalculatedHoursWorked") > 8)
        summary_df.write.format("delta").mode("overwrite").save("/delta/employee_attendance_summary")
        overtime_df.write.format("delta").mode("overwrite").save("/delta/employee_overtime")
    except Exception as e:
        print(f"Error during summarization: {e}")

# File path to the raw data
file_path = "/path/to/employee_attendance.csv"

# Execute the pipeline
df_raw = ingest_attendance_data(file_path)
if df_raw is not None:
    df_cleaned = clean_attendance_data(df_raw)
    if df_cleaned is not None:
        summarize_attendance(df_cleaned)
```

### Task 5: Time Travel with Delta Lake
```python
# Assuming you want to roll back the attendance logs
spark.sql("CREATE OR REPLACE TABLE attendance_logs AS SELECT * FROM delta.`/mnt/delta/attendance_logs` VERSION AS OF <version_number>")

# Display the history of the attendance logs
spark.sql("DESCRIBE HISTORY delta.`/mnt/delta/attendance_logs`").show(truncate=False)
```
