# Assignment 2: Structured Streaming and Transformations on Streams 

### Task 1 Ingest Streaming Data from CSV Files

#### 1. Create a folder for streaming CSV files
In Databricks, create a folder to store your streaming CSV files. You can use DBFS (Databricks File System) for this purpose.
```python
# Create a folder in DBFS
dbutils.fs.mkdirs(mntstreamingcsv)
```

#### 2. Set up a structured streaming source to continuously read CSV data from this folder
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# Define the schema for the CSV files
schema = StructType([
    StructField(TransactionID, StringType(), True),
    StructField(TransactionDate, DateType(), True),
    StructField(ProductID, StringType(), True),
    StructField(Quantity, IntegerType(), True),
    StructField(Price, IntegerType(), True)
])

# Create a streaming DataFrame that reads from the CSV folder
streaming_df = spark.readStream.schema(schema).csv(mntstreamingcsv)

# Display the streaming DataFrame in the console
query = streaming_df.writeStream.outputMode(append).format(console).start()
query.awaitTermination()
```

### Task 2 Stream Transformations

#### 1. Perform transformations on the incoming data
```python
from pyspark.sql.functions import col, expr

# Add a new column for TotalAmount and filter records
transformed_df = streaming_df.withColumn(TotalAmount, col(Quantity)  col(Price)) 
    .filter(col(Quantity)  1)

# Write the transformed stream to a memory sink
query = transformed_df.writeStream.outputMode(append).format(memory).queryName(transformed_stream).start()
query.awaitTermination()
```

#### 2. View results in memory sink
```python
# Query the memory sink to view results
spark.sql(SELECT  FROM transformed_stream).show()
```

### Task 3 Aggregations on Streaming Data

#### 1. Implement an aggregation on the streaming data
```python
# Group by ProductID and calculate total sales
aggregated_df = transformed_df.groupBy(ProductID) 
    .agg(expr(sum(TotalAmount) as TotalSales))

# Write the aggregated data to a memory sink
query = aggregated_df.writeStream.outputMode(update).format(memory).queryName(aggregated_stream).start()
query.awaitTermination()
```

#### 2. View the aggregated results in the memory sink
```python
# Query the memory sink to view aggregated results
spark.sql(SELECT  FROM aggregated_stream).show()
```

### Task 4 Writing Streaming Data to File Sinks

#### 1. Write the streaming results to a Parquet sink
```python
# Define the checkpoint location
checkpoint_dir = mntstreamingcheckpoint

# Write the transformed data to Parquet
query = transformed_df.writeStream 
    .format(parquet) 
    .option(checkpointLocation, checkpoint_dir) 
    .option(path, mntstreamingoutput) 
    .start()
query.awaitTermination()
```

### Task 5 Handling Late Data using Watermarks

#### 1. Introduce a watermark to handle late data
```python
# Add watermark and handle late data
watermarked_df = transformed_df.withWatermark(TransactionDate, 1 day) 
    .groupBy(ProductID) 
    .agg(expr(sum(TotalAmount) as TotalSales))

# Write the watermarked data to Parquet
query = watermarked_df.writeStream 
    .format(parquet) 
    .option(checkpointLocation, checkpoint_dir) 
    .option(path, mntstreamingwatermarked_output) 
    .start()
query.awaitTermination()
```

### Task 6 Streaming from Multiple Sources

#### 1. Simulate multiple streams
```python
# Define schema for product information
product_schema = StructType([
    StructField(ProductID, StringType(), True),
    StructField(ProductName, StringType(), True),
    StructField(Category, StringType(), True)
])

# Create streaming DataFrame for product data
product_stream_df = spark.readStream.schema(product_schema).csv(mntstreamingproducts)

# Perform a join on the two streams
joined_df = transformed_df.join(product_stream_df, ProductID)

# Write the joined stream to a memory sink
query = joined_df.writeStream.outputMode(append).format(memory).queryName(joined_stream).start()
query.awaitTermination()
```

#### 2. View the joined stream results in the memory sink
```python
# Query the memory sink to view the joined results
spark.sql(SELECT  FROM joined_stream).show()
```

### Task 7 Stopping and Restarting Streaming Queries

#### 1. Stop the streaming query and explore the results
```python
# Stop the streaming query
query.stop()

# Explore the results in the output directory or memory sink
spark.sql(SELECT  FROM transformed_stream).show()
```

#### 2. Restart the query and continue from the last processed data
```python
# Restart the streaming query
query = transformed_df.writeStream 
    .format(parquet) 
    .option(checkpointLocation, checkpoint_dir) 
    .option(path, mntstreamingoutput) 
    .start()
query.awaitTermination()
```
