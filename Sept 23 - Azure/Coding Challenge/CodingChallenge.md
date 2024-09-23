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

---

# Movie Ratings Data Ingestion and Analysis

## CSV Data
```csv
UserID,MovieID,Rating,Timestamp
U001,M001,4,2024-05-01 14:30:00
U002,M002,5,2024-05-01 16:00:00
U003,M001,3,2024-05-02 10:15:00
U001,M003,2,2024-05-02 13:45:00
U004,M002,4,2024-05-03 18:30:00
```

## Task 1: Movie Ratings Data Ingestion
Ingest the CSV data into a Delta table in Databricks. Ensure proper error handling for missing or inconsistent data, and log errors accordingly.

```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MovieRatings") \
    .getOrCreate()

# Define the CSV file path
file_path = "/path/to/movie_ratings.csv"

# Try to read the CSV file into a DataFrame
try:
    df = spark.read.format("csv").option("header", "true").load(file_path)
    df.write.format("delta").mode("overwrite").save("/delta/movie_ratings")
except AnalysisException as e:
    print(f"Error: {e}")
    # Log the error to a file or monitoring system
```

## Task 2: Data Cleaning
Clean the movie ratings data to ensure that the `Rating` column contains values between 1 and 5, and remove duplicates based on `UserID` and `MovieID`. Save the cleaned data to a new Delta table.

```python
# Load the Delta table
df = spark.read.format("delta").load("/delta/movie_ratings")

# Data cleaning
cleaned_df = df.filter((df.Rating >= 1) & (df.Rating <= 5)) \
                .dropDuplicates(["UserID", "MovieID"])

# Save cleaned data to a new Delta table
cleaned_df.write.format("delta").mode("overwrite").save("/delta/cleaned_movie_ratings")
```

## Task 3: Movie Rating Analysis
Analyze the movie ratings to calculate the average rating for each movie and identify movies with the highest and lowest average ratings. Save the analysis results to a Delta table.

```python
# Load the cleaned Delta table
cleaned_df = spark.read.format("delta").load("/delta/cleaned_movie_ratings")

# Calculate average rating per movie
average_rating_df = cleaned_df.groupBy("MovieID").agg({"Rating": "avg"}).withColumnRenamed("avg(Rating)", "AverageRating")

# Identify movies with the highest and lowest average ratings
highest_rating = average_rating_df.orderBy("AverageRating", ascending=False).first()
lowest_rating = average_rating_df.orderBy("AverageRating").first()

# Save analysis results to a Delta table
average_rating_df.write.format("delta").mode("overwrite").save("/delta/movie_rating_analysis")
```

## Task 4: Time Travel and Delta Lake History
Implement Delta Lake's time travel feature by performing an update to the movie ratings data, rolling back to a previous version to retrieve the original ratings, and using `DESCRIBE HISTORY` to view the changes.

```python
# Load the original Delta table
df = spark.read.format("delta").load("/delta/movie_ratings")

# Update some ratings
updated_df = df.withColumn("Rating", when(df.UserID == "U001", 5).otherwise(df.Rating))
updated_df.write.format("delta").mode("overwrite").save("/delta/movie_ratings")

# Roll back to the previous version
df_rollback = spark.read.format("delta").option("versionAsOf", 0).load("/delta/movie_ratings")

# Use DESCRIBE HISTORY to check changes
history_df = spark.sql("DESCRIBE HISTORY delta.`/delta/movie_ratings`")
history_df.show()
```

## Task 5: Optimize Delta Table
Apply optimizations to the Delta table, implementing Z-ordering on the `MovieID` column, compacting the data, and cleaning up older versions.

```python
# Optimize the Delta table with Z-ordering
spark.sql("OPTIMIZE delta.`/delta/movie_ratings` ZORDER BY (MovieID)")

# Use VACUUM to clean up old data
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/delta/movie_ratings")
deltaTable.vacuum(retentionHours=168)  # Retain data for a week
```

---

# Data Ingestion and Processing

## Task 1: Data Ingestion - Reading Data from Various Formats
1. **Ingest data from different formats:**

   **CSV Data:** Use the following CSV data to represent student information:
   ```csv
   StudentID,Name,Class,Score
   S001,Anil Kumar,10,85
   S002,Neha Sharma,12,92
   S003,Rajesh Gupta,11,78
   ```

   **JSON Data:** Use the following JSON data to represent city information:
   ```json
   [
       {"CityID": "C001", "CityName": "Mumbai", "Population": 20411000},
       {"CityID": "C002", "CityName": "Delhi", "Population": 16787941},
       {"CityID": "C003", "CityName": "Bangalore", "Population": 8443675}
   ]
   ```

   **Parquet Data:** Load a dataset containing data about hospitals stored in Parquet format.
   
   **Delta Table:** Load a Delta table containing hospital records, including proper error handling if the table does not exist.

   ```python
   from pyspark.sql import SparkSession
   from pyspark.sql.utils import AnalysisException

   # Initialize Spark session
   spark = SparkSession.builder \
       .appName("DataIngestion") \
       .getOrCreate()

   # Ingest CSV Data
   csv_data = spark.read.format("csv").option("header", "true").load("/path/to/student_data.csv")

   # Ingest JSON Data
   json_data = spark.read.json("/path/to/city_data.json")

   # Ingest Parquet Data
   parquet_data = spark.read.format("parquet").load("/path/to/hospital_data.parquet")

   # Ingest Delta Table with error handling
   try:
       delta_data = spark.read.format("delta").load("/path/to/hospital_records")
   except AnalysisException as e:
       print(f"Error loading Delta table: {e}")
   ```

## Task 2: Writing Data to Various Formats
1. **Write data to different formats:**

   **CSV:** Write the student data to a CSV file.
   
   **JSON:** Write the city data to a JSON file.
   
   **Parquet:** Write the hospital data to a Parquet file.
   
   **Delta Table:** Write the hospital data to a Delta table.

   ```python
   # Write student data to CSV
   csv_data.write.format("csv").option("header", "true").save("/output/student_data.csv")

   # Write city data to JSON
   json_data.write.format("json").save("/output/city_data.json")

   # Write hospital data to Parquet
   parquet_data.write.format("parquet").save("/output/hospital_data.parquet")

   # Write hospital data to Delta table
   parquet_data.write.format("delta").mode("overwrite").save("/delta/hospital_records")
   ```

## Task 3: Running One Notebook from Another
1. **Create two notebooks:**
   
   **Notebook A:** Ingest data from a CSV file, clean it, and save it as a Delta table.
   
   **Notebook B:** Perform analysis on the Delta table created in Notebook A.

   ```python
   # Notebook A
   student_data = spark.read.format("csv").option("header", "true").load("/path/to/student_data.csv")
   cleaned_data = student_data.dropDuplicates().na.fill("N/A")
   cleaned_data.write.format("delta").mode("overwrite").save("/delta/cleaned_student_data")
   ```

   ```python
   # Notebook B
   cleaned_data = spark.read.format("delta").load("/delta/cleaned_student_data")
   average_score = cleaned_data.groupBy("Class").agg({"Score": "avg"})
   average_score.write.format("delta").mode("overwrite").save("/delta/average_student_score")
   ```

2. **Run Notebook B from Notebook A:**
   ```python
   # In Notebook A, run Notebook B
   dbutils.notebook.run("/path/to/NotebookB", 60)
   ```

## Task 4: Databricks Ingestion
1. **Read data from various sources:**
   - CSV file from Azure Data Lake.
   - JSON file stored in Databricks FileStore.
   - Parquet file from an external data source (e.g., AWS S3).
   - Delta table stored in a Databricks-managed database.

   ```python
   azure_csv_data = spark.read.format("csv").option("header", "true").load("abfss://<container>@<account>.dfs.core.windows.net/path/to/azure_data.csv")
   databricks_json_data = spark.read.json("/FileStore/path/to/databricks_data.json")
   external_parquet_data = spark.read.format("parquet").load("s3://bucket/path/to/external_data.parquet")
   managed_delta_data = spark.read.format("delta").load("/delta/managed_database/delta_table")
   ```

2. **Write cleaned data to each format:**
   ```python
   cleaned_data.write.format("csv").save("/output/cleaned_data.csv")
   cleaned_data.write.format("json").save("/output/cleaned_data.json")
   cleaned_data.write.format("parquet").save("/output/cleaned_data.parquet")
   cleaned_data.write.format("delta").save("/delta/cleaned_data")
   ```

   **Additional Tasks:**
   - **Optimization Task:** Optimize the Delta table using the OPTIMIZE command.
   - **Z-ordering Task:** Apply Z-ordering on the Class column.
   - **Vacuum Task:** Use the VACUUM command to clean up old versions of the Delta table.

   ```python
   spark.sql("OPTIMIZE delta.`/delta/cleaned_data`")
   spark.sql("OPTIMIZE delta.`/delta/cleaned_data` ZORDER BY (Class)")
   deltaTable = DeltaTable.forPath(spark, "/delta/cleaned_data")
   deltaTable.vacuum(retentionHours=168)  # Retain data for a week
   ```

---

# Exercise 1: Creating a Complete ETL Pipeline using Delta Live Tables (DLT)

## Objective
Learn how to create an end-to-end ETL pipeline using Delta Live Tables.

## Tasks

### 1. Create Delta Live Table (DLT) Pipeline
Set up a DLT pipeline for processing transactional data. Use sample data representing daily customer transactions.

**Sample Data:**
```csv
TransactionID,TransactionDate,CustomerID,Product,Quantity,Price
1,2024-09-01,C001,Laptop,1,1200
2,2024-09-02,C002,Tablet,2,300
3,2024-09-03,C001,Headphones,5,50
4,2024-09-04,C003,Smartphone,1,800
5,2024-09-05,C004,Smartwatch,3,200
```

**Pipeline Steps:**
- **Step 1:** Ingest raw data from CSV files.
- **Step 2:** Apply transformations (e.g., calculate total transaction amount).
- **Step 3:** Write the final data into a Delta table.

### 2. Write DLT in Python
Implement the pipeline using DLT in Python. Define the following tables:
- **Raw Transactions Table:** Read data from the CSV file.
- **Transformed Transactions Table:** Apply transformations.

```python
import dlt

@dlt.table
def raw_transactions():
    return dlt.read_csv("path/to/transactions.csv")

@dlt.table
def transformed_transactions():
    raw_data = dlt.read("raw_transactions")
    return raw_data.with_columns(
        (raw_data.Quantity * raw_data.Price).alias("TotalAmount")
    )
```

### 3. Write DLT in SQL
Implement the same pipeline using DLT in SQL.

```sql
CREATE TABLE raw_transactions
USING CSV
OPTIONS (path "path/to/transactions.csv");

CREATE TABLE transformed_transactions AS
SELECT *, Quantity * Price AS TotalAmount
FROM raw_transactions;
```

### 4. Monitor the Pipeline
Use Databricks' DLT UI to monitor the pipeline and check the status of each step.

---

# Exercise 2: Delta Lake Operations - Read, Write, Update, Delete, Merge

## Objective
Work with Delta Lake to perform read, write, update, delete, and merge operations using both PySpark and SQL.

## Tasks

### 1. Read Data from Delta Lake
Read the transactional data from the Delta table you created in the first exercise using PySpark and SQL.

```python
# Using PySpark
transactions_df = spark.read.format("delta").load("/delta/transactions")
transactions_df.show(5)
```

```sql
-- Using SQL
SELECT * FROM delta.`/delta/transactions` LIMIT 5;
```

### 2. Write Data to Delta Lake
Append new transactions to the Delta table using PySpark.

**New Transactions:**
```csv
TransactionID,TransactionDate,CustomerID,Product,Quantity,Price
6,2024-09-06,C005,Keyboard,4,100
7,2024-09-07,C006,Mouse,10,20
```

```python
new_transactions = spark.createDataFrame([
    (6, "2024-09-06", "C005", "Keyboard", 4, 100),
    (7, "2024-09-07", "C006", "Mouse", 10, 20)
], ["TransactionID", "TransactionDate", "CustomerID", "Product", "Quantity", "Price"])

new_transactions.write.format("delta").mode("append").save("/delta/transactions")
```

### 3. Update Data in Delta Lake
Update the Price of Product = 'Laptop' to 1300.

```python
# Using PySpark
spark.sql("UPDATE delta.`/delta/transactions` SET Price = 1300 WHERE Product = 'Laptop'")
```

### 4. Delete Data from Delta Lake
Delete all transactions where the Quantity is less than 3.

```python
# Using PySpark
spark.sql("DELETE FROM delta.`/delta/transactions` WHERE Quantity < 3")
```

### 5. Merge Data into Delta Lake
Create new data representing updates and merge it.

**New Data:**
```csv
TransactionID,TransactionDate,CustomerID,Product,Quantity,Price
1,2024-09-01,C001,Laptop,1,1250 -- Updated Price
8,2024-09-08,C007,Charger,2,30 -- New Transaction
```

```python
merge_data = spark.createDataFrame([
    (1, "2024-09-01", "C001", "Laptop", 1, 1250),
    (8, "2024-09-08", "C007", "Charger", 2, 30)
], ["TransactionID", "TransactionDate", "CustomerID", "Product", "Quantity", "Price"])

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/delta/transactions")
delta_table.alias("t").merge(
    merge_data.alias("s"),
    "t.TransactionID = s.TransactionID"
).whenMatchedUpdate(set={
    "TransactionDate": "s.TransactionDate",
    "CustomerID": "s.CustomerID",
    "Product": "s.Product",
    "Quantity": "s.Quantity",
    "Price": "s.Price"
}).whenNotMatchedInsert(values={
    "TransactionID": "s.TransactionID",
    "TransactionDate": "s.TransactionDate",
    "CustomerID": "s.CustomerID",
    "Product": "s.Product",
    "Quantity": "s.Quantity",
    "Price": "s.Price"
}).execute()
```

---

# Exercise 3: Delta Lake - History, Time Travel, and Vacuum

## Objective
Understand how to use Delta Lake features such as versioning, time travel, and data cleanup with vacuum.

## Tasks

### 1. View Delta Table History
Query the history of the Delta table.

```python
# Using PySpark
history_df = spark.sql("DESCRIBE HISTORY delta.`/delta/transactions`")
history_df.show()
```

```sql
-- Using SQL
DESCRIBE HISTORY delta.`/delta/transactions`;
```

### 2. Perform Time Travel
Retrieve the state of the Delta table as it was 5 versions ago.

```python
# Using PySpark
old_version_df = spark.read.format("delta").option("versionAsOf", 5).load("/delta/transactions")
old_version_df.show()
```

### 3. Vacuum the Delta Table
Clean up old data using the VACUUM command.

```python
spark.sql("VACUUM delta.`/delta/transactions` RETAIN 168 HOURS")  # 7 days
```

### 4. Converting Parquet Files to Delta Files
Create a new Parquet-based table and convert it to Delta.

```python
# Load from Parquet
parquet_df = spark.read.format("parquet").load("/path/to/parquet_table")

# Write as Delta
parquet_df.write.format("delta").mode("overwrite").save("/delta/new_table")
```

---

# Exercise 4: Implementing Incremental Load Pattern using Delta Lake

## Objective
Learn how to implement incremental data loading with Delta Lake to avoid reprocessing old data.

## Tasks

### 1. Set Up Initial Data
Load transactions from the first three days into the Delta table.

```python
initial_data = spark.createDataFrame([
    (1, "2024-09-01", "C001", "Laptop", 1, 1200),
    (2, "2024-09-02", "C002", "Tablet", 2, 300),
    (3, "2024-09-03", "C001", "Headphones", 5, 50)
], ["TransactionID", "TransactionDate", "CustomerID", "Product", "Quantity", "Price"])

initial_data.write.format("delta").mode("overwrite").save("/delta/transactions")
```

### 2. Set Up Incremental Data
Add new transactions representing the next four days.

```python
incremental_data = spark.createDataFrame([
    (4, "2024-09-04", "C003", "Smartphone", 1, 800),
    (5, "2024-09-05", "C004", "Smartwatch", 3, 200),
    (6, "2024-09-06", "C005", "Keyboard", 4, 100),
    (7, "2024-09-07", "C006", "Mouse", 10, 20)
], ["TransactionID", "TransactionDate", "CustomerID", "Product", "Quantity", "Price"])

incremental_data.write.format("delta").mode("append").save("/delta/transactions")
```

### 3. Implement Incremental Load
Create a pipeline to read new transactions only and append them.

```python
# Load only new transactions
new_transactions = spark.read.format("delta").load("/delta/transactions").filter("TransactionDate > '2024-09-03'")
new_transactions.write.format("delta").mode("append").save("/delta/transactions")
```

### 4. Monitor Incremental Load
Check the Delta Lake version history to ensure only new transactions are added.

```python
version_history = spark.sql("DESCRIBE HISTORY delta.`/delta/transactions`")
version_history.show()
```
