# Vehicle Maintenance Data Ingestion and Analysis

## CSV Data
```csv
VehicleID,Date,ServiceType,ServiceCost,Mileage
V001,2024-04-01,Oil Change,50.00,15000
V002,2024-04-05,Tire Replacement,400.00,30000
V003,2024-04-10,Battery Replacement,120.00,25000
V004,2024-04-15,Brake Inspection,200.00,40000
V005,2024-04-20,Oil Change,50.00,18000
```

## Task 1: Vehicle Maintenance Data Ingestion
Ingest the CSV data into a Delta table in Databricks, ensuring error handling for missing files or incorrect data.

```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder \
    .appName("VehicleMaintenance") \
    .getOrCreate()

# Define the CSV file path
file_path = "/path/to/vehicle_maintenance.csv"

# Try to read the CSV file into a DataFrame
try:
    df = spark.read.format("csv").option("header", "true").load(file_path)
    df.write.format("delta").mode("overwrite").save("/delta/vehicle_maintenance")
except AnalysisException as e:
    print(f"Error: {e}")
    # Log the error to a file or monitoring system
```

## Task 2: Data Cleaning
Clean the vehicle maintenance data to ensure that `ServiceCost` and `Mileage` are valid positive values, and remove duplicates based on `VehicleID` and `Date`. Save the cleaned data to a new Delta table.

### Code
```python
# Load the Delta table
df = spark.read.format("delta").load("/delta/vehicle_maintenance")

# Data cleaning
cleaned_df = df.filter((df.ServiceCost > 0) & (df.Mileage > 0)) \
                .dropDuplicates(["VehicleID", "Date"])

# Save cleaned data to a new Delta table
cleaned_df.write.format("delta").mode("overwrite").save("/delta/cleaned_vehicle_maintenance")
```

## Task 3: Vehicle Maintenance Analysis
Analyze the vehicle maintenance data to calculate the total maintenance cost for each vehicle and identify vehicles exceeding a certain mileage threshold. Save the analysis results to a Delta table.

```python
# Load the cleaned Delta table
cleaned_df = spark.read.format("delta").load("/delta/cleaned_vehicle_maintenance")

# Calculate total maintenance cost per vehicle
maintenance_cost_df = cleaned_df.groupBy("VehicleID").agg({"ServiceCost": "sum"}).withColumnRenamed("sum(ServiceCost)", "TotalCost")

# Identify vehicles exceeding 30,000 miles
vehicles_needing_service = cleaned_df.filter(cleaned_df.Mileage > 30000)

# Save analysis results to a Delta table
maintenance_cost_df.write.format("delta").mode("overwrite").save("/delta/maintenance_cost_analysis")
vehicles_needing_service.write.format("delta").mode("overwrite").save("/delta/vehicles_needing_service")
```

## Task 4: Data Governance with Delta Lake
Enable Delta Lake's data governance features by cleaning up old data and checking the history of updates to the maintenance records.

```python
# Clean up old data from the Delta table
from delta.tables import *

# Use VACUUM
deltaTable = DeltaTable.forPath(spark, "/delta/cleaned_vehicle_maintenance")
deltaTable.vacuum(retentionHours=168)  # Retain data for a week

# Check the history of updates to the maintenance records
history_df = spark.sql("DESCRIBE HISTORY delta.`/delta/cleaned_vehicle_maintenance`")
history_df.show()
```
