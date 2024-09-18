# 3 - Creating a Complete ETL Pipeline using Delta Live Tables (DLT)

## Objective
This assignment aims to help participants learn to create a complete ETL (Extract, Transform, Load) pipeline using Databricks Delta Live Tables (DLT). Participants will explore SQL and PySpark methods for DLT pipelines, perform basic operations like read, write, update, delete, and merge, and explore Delta Lake's advanced features.

## Task 1: Create an ETL Pipeline using DLT (Python)

### 1. Create a Delta Live Table Pipeline

- **Read the source data** from a CSV or Parquet file.
- **Transform the data**:
  - Add a new column `TotalAmount` (Quantity * Price).
  - Filter records where `Quantity` > 1.
- **Load the transformed data** into a Delta table.

#### Dataset
```csv
OrderID,OrderDate,CustomerID,Product,Quantity,Price
101,2024-01-01,C001,Laptop,2,1000
102,2024-01-02,C002,Phone,1,500
103,2024-01-03,C003,Tablet,3,300
104,2024-01-04,C004,Monitor,1,150
105,2024-01-05,C005,Mouse,5,20
```

### Code Snippet
```python
# PySpark code for Task 1
from pyspark.sql.functions import col

# Read CSV data
df = spark.read.csv("/mnt/delta/orders", header=True, inferSchema=True)

# Transform data
transformed_df = df.withColumn("TotalAmount", col("Quantity") * col("Price")) \
    .filter(col("Quantity") > 1)

# Write to Delta Table
transformed_df.write.format("delta").mode("overwrite").save("/mnt/delta/orders_delta")
```

## Task 2: Create an ETL Pipeline using DLT (SQL)

### 1. Create Delta Live Table Pipeline using SQL

- **Read the source data** using SQL.
- **Perform transformations** similar to Task 1.
- **Write the data** into a Delta table.

### SQL Code Snippet
```sql
-- SQL code for Task 2
CREATE OR REPLACE LIVE TABLE orders_delta AS
SELECT OrderID, OrderDate, CustomerID, Product, Quantity, Price,
       Quantity * Price AS TotalAmount
FROM orders
WHERE Quantity > 1;
```

## Task 3: Perform Read, Write, Update, and Delete Operations on Delta Table (SQL + PySpark)

### 1. Read Data
```python
# PySpark code to read Delta table
df = spark.read.format("delta").load("/mnt/delta/orders_delta")
df.show()
```

### 2. Update Table
```sql
-- SQL code to update Delta table
UPDATE orders_delta
SET Price = Price * 1.1
WHERE Product = 'Laptop';
```

### 3. Delete Rows
```sql
-- SQL code to delete rows from Delta table
DELETE FROM orders_delta
WHERE Quantity < 2;
```

### 4. Insert New Record
```python
# PySpark code to insert a new record
new_data = spark.createDataFrame([(106, '2024-01-06', 'C007', 'Keyboard', 3, 50)], schema=df.columns)
new_data.write.format("delta").mode("append").save("/mnt/delta/orders_delta")
```

## Task 4: Merge Data (SCD Type 2)

### 1. Implement MERGE Operation

#### New Data
```csv
OrderID,OrderDate,CustomerID,Product,Quantity,Price
101,2024-01-10,C001,Laptop,2,1200
106,2024-01-12,C006,Keyboard,3,50
```

### SQL Code Snippet
```sql
-- SQL code for SCD Type 2 Merge
MERGE INTO orders_delta AS target
USING (SELECT * FROM new_orders) AS source
ON target.OrderID = source.OrderID
WHEN MATCHED THEN
    UPDATE SET Quantity = source.Quantity,
               Price = source.Price,
               TotalAmount = source.Quantity * source.Price
WHEN NOT MATCHED THEN
    INSERT (OrderID, OrderDate, CustomerID, Product, Quantity, Price, TotalAmount)
    VALUES (source.OrderID, source.OrderDate, source.CustomerID, source.Product, source.Quantity, source.Price, source.Quantity * source.Price);
```

## Task 5: Explore Delta Table Internals

### 1. Inspect Delta Table’s Transaction Logs

```sql
-- SQL code to show Delta table history
DESCRIBE HISTORY orders_delta;
```

### 2. Check File Size and Modification Times

```sql
-- SQL code to describe Delta table details
DESCRIBE DETAIL orders_delta;
```

## Task 6: Time Travel in Delta Tables

### 1. Use Time Travel

```sql
-- SQL code to query Delta table as of a specific version
SELECT * FROM orders_delta VERSION AS OF 1;

-- SQL code to query Delta table as of a specific timestamp
SELECT * FROM orders_delta TIMESTAMP AS OF '2024-01-05 00:00:00';
```

## Task 7: Optimize Delta Table

### 1. Optimize Table with Z-Ordering

```sql
-- SQL code to optimize Delta table
OPTIMIZE orders_delta ZORDER BY (Product);
```

### 2. Use Vacuum to Remove Old Files

```sql
-- SQL code to vacuum Delta table
VACUUM orders_delta RETAIN 168 HOURS;
```

## Task 8: Converting Parquet Files to Delta Format

### 1. Convert Parquet Files

```python
# PySpark code to convert Parquet to Delta
parquet_df = spark.read.format("parquet").load("/mnt/delta/historical_orders_parquet")
parquet_df.write.format("delta").mode("overwrite").save("/mnt/delta/historical_orders_delta")
```

### 2. Verify Conversion

```python
# PySpark code to query the converted Delta table
delta_df = spark.read.format("delta").load("/mnt/delta/historical_orders_delta")
delta_df.show()
```
