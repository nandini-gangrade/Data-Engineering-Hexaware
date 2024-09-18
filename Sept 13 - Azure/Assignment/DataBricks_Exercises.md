# Databricks Tasks Code

## 1. Introduction to Databricks
### Task: Creating a Databricks Notebook

```python
# Create a new Databricks notebook
# Use Python code to demonstrate basic operations

import pandas as pd

# Sample data
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'Salary': [70000, 80000, 120000]
}

# Create DataFrame
df = pd.DataFrame(data)

# Perform basic operations
mean_salary = df['Salary'].mean()
sum_salary = df['Salary'].sum()

print(f"Mean Salary: {mean_salary}")
print(f"Sum Salary: {sum_salary}")
```

## 2. Setting Up Azure Databricks Workspace and Configuring Clusters
### Task: Configuring Clusters

```python
# Attach the notebook to the cluster
# Run basic Python code to verify configuration

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Cluster Configuration Check") \
    .getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Perform basic operation
df.show()
df.select(F.avg("Age")).show()
```

## 3. Real-Time Data Processing with Databricks
### Task: Implementing Databricks for Real-Time Data Processing

```python
# Real-time data processing using streaming APIs
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Real-Time Data Processing") \
    .getOrCreate()

# Read streaming data from a socket (for simulation purposes)
streamingDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Process the streaming data (parsing and aggregation)
processedDF = streamingDF.selectExpr("CAST(value AS STRING) AS json") \
    .select(F.from_json(F.col("json"), "event_time STRING, event_type STRING, user_id STRING, amount DOUBLE").alias("data")) \
    .select("data.*") \
    .groupBy(window(F.col("event_time"), "1 minute"), F.col("event_type")) \
    .agg(sum("amount").alias("total_amount"))

# Output the result to the console
query = processedDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

## 4. Data Exploration and Visualization in Databricks
### Task: Visualizing Data in Databricks

```python
# Exploratory Data Analysis (EDA) and visualization
import matplotlib.pyplot as plt
import pandas as pd

# Sample dataset
data = {
    'Product': ['A', 'B', 'C', 'D', 'E'],
    'Sales': [200, 300, 400, 500, 600]
}

df = pd.DataFrame(data)

# Bar chart
df.plot(kind='bar', x='Product', y='Sales', title='Product Sales')
plt.show()

# Scatter plot
df.plot(kind='scatter', x='Product', y='Sales', title='Sales Scatter Plot')
plt.show()
```

## 6. Reading and Writing Data in Databricks
### Task: Reading and Writing Data in Various Formats

```python
# Reading and writing data in different formats
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read and Write Data") \
    .getOrCreate()

# Read data from various formats
csvDF = spark.read.csv("path/to/data.csv", header=True, inferSchema=True)
jsonDF = spark.read.json("path/to/data.json")
parquetDF = spark.read.parquet("path/to/data.parquet")

# Write data to Delta format
csvDF.write.format("delta").save("path/to/delta_table")

# Write data to other formats
csvDF.write.format("parquet").save("path/to/output_parquet")
jsonDF.write.format("json").save("path/to/output_json")
```

## 7. Analyzing and Visualizing Streaming Data with Databricks
### Task: Analyzing Streaming Data

```python
# Analyzing streaming data
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Streaming Data Analysis") \
    .getOrCreate()

# Read streaming data from a socket (for simulation purposes)
streamingDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Process the streaming data
processedDF = streamingDF.selectExpr("CAST(value AS STRING) AS json") \
    .select(F.from_json(F.col("json"), "event_time STRING, event_type STRING, user_id STRING, amount DOUBLE").alias("data")) \
    .select("data.*") \
    .groupBy(window(F.col("event_time"), "1 minute"), F.col("event_type")) \
    .agg(sum("amount").alias("total_amount"))

# Output the result to the console
query = processedDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

## 8. Introduction to Databricks Delta Lake
### Task: Using Delta Lake for Data Versioning

```python
# Using Delta Lake for data versioning
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Versioning") \
    .getOrCreate()

# Create a Delta table
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.write.format("delta").save("path/to/delta_table")

# Update the Delta table
updates = [("Alice", 26), ("Bob", 31)]
updateDF = spark.createDataFrame(updates, columns)
updateDF.write.format("delta").mode("append").save("path/to/delta_table")

# Demonstrate time travel
deltaTable = DeltaTable.forPath(spark, "path/to/delta_table")
deltaTable.history().show()
```

## 9. Managed and Unmanaged Tables
### Task: Creating Managed and Unmanaged Tables

```python
# Creating managed and unmanaged tables
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Managed and Unmanaged Tables") \
    .getOrCreate()

# Create a managed table
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.write.mode("overwrite").saveAsTable("managed_table")

# Create an unmanaged table
df.write.mode("overwrite").format("parquet").save("path/to/unmanaged_table")

# Load and query the managed table
managedDF = spark.sql("SELECT * FROM managed_table")
managedDF.show()

# Load and query the unmanaged table
unmanagedDF = spark.read.format("parquet").load("path/to/unmanaged_table")
unmanagedDF.show()
```

## 10. Views and Temporary Views
### Task: Working with Views in Databricks

```python
# Working with views in Databricks
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Views in Databricks") \
    .getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("temp_view")
df.createOrReplaceGlobalTempView("global_temp_view")

# Create a view
spark.sql("CREATE VIEW my_view AS SELECT * FROM temp_view")

# Query the views
spark.sql("SELECT * FROM my_view").show()
spark.sql("SELECT * FROM global_temp_view").show()
```
