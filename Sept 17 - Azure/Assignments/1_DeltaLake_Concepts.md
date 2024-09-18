### Task 1: Creating Delta Table using Three Methods

#### 1. Load the given CSV and JSON datasets into Databricks

**CSV Dataset**: Employees
```python
# Load CSV data into a DataFrame
csv_path = "/path/to/employees.csv"
employees_df = spark.read.format("csv").option("header", "true").load(csv_path)
```

**JSON Dataset**: Products
```python
# Load JSON data into a DataFrame
json_path = "/path/to/products.json"
products_df = spark.read.format("json").load(json_path)
```

#### 2. Create a Delta table using the following three methods

**a. Create a Delta table from a DataFrame**
```python
# Write the DataFrame to a Delta table
employees_df.write.format("delta").mode("overwrite").save("/path/to/delta/employees")
```

**b. Use SQL to create a Delta table**
```sql
-- Create a Delta table using SQL
CREATE TABLE delta_employees
USING delta
LOCATION '/path/to/delta/employees'
AS SELECT * FROM csv.`/path/to/employees.csv`
```

**c. Convert both the CSV and JSON files into Delta format**
```python
# Convert CSV to Delta format
employees_df.write.format("delta").mode("overwrite").save("/path/to/delta/employees")

# Convert JSON to Delta format
products_df.write.format("delta").mode("overwrite").save("/path/to/delta/products")
```

### Task 2: Merge and Upsert (Slowly Changing Dimension - SCD)

#### 1. Load the Delta table for employees
```python
# Load the existing Delta table
employees_delta_df = spark.read.format("delta").load("/path/to/delta/employees")
```

#### 2. Merge the new employee data into the employees Delta table

**New Employee Data**
```python
# Load new employee data
new_employees_data = [
    (102, "Alice", "Finance", "2023-02-15", 75000),
    (106, "Olivia", "HR", "2023-06-10", 65000)
]
new_employees_df = spark.createDataFrame(new_employees_data, ["EmployeeID", "EmployeeName", "Department", "JoiningDate", "Salary"])

# Perform upsert (merge) operation
from delta.tables import DeltaTable

delta_employees = DeltaTable.forPath(spark, "/path/to/delta/employees")

delta_employees.alias("e").merge(
    new_employees_df.alias("n"),
    "e.EmployeeID = n.EmployeeID"
).whenMatchedUpdate(set={
    "Salary": "n.Salary"
}).whenNotMatchedInsert(values={
    "EmployeeID": "n.EmployeeID",
    "EmployeeName": "n.EmployeeName",
    "Department": "n.Department",
    "JoiningDate": "n.JoiningDate",
    "Salary": "n.Salary"
}).execute()
```

### Task 3: Internals of Delta Table

#### 1. Explore the internals of the employees Delta table
```python
# Show the Delta table's metadata
delta_employees.describe().show()
```

#### 2. Check the transaction history of the table
```python
# Get the transaction history of the Delta table
delta_employees.history().show(truncate=False)
```

#### 3. Perform Time Travel and retrieve the table before the previous merge operation
```python
# Retrieve the table as of a specific version (e.g., version 0)
historical_df = spark.read.format("delta").option("versionAsOf", 0).load("/path/to/delta/employees")
historical_df.show()
```

### Task 4: Optimize Delta Table

#### 1. Optimize the employees Delta table for better performance
```python
# Optimize the Delta table
from delta.tables import DeltaTable

delta_employees.optimize().executeCompaction()
```

#### 2. Use Z-ordering on the Department column
```python
# Z-order the table on the Department column
delta_employees.optimize().zOrderBy("Department")
```

### Task 5: Time Travel with Delta Table

#### 1. Retrieve the employees Delta table as it was before the last merge
```python
# Query the table at a specific version before the last merge
past_version_df = spark.read.format("delta").option("versionAsOf", previous_version).load("/path/to/delta/employees")
past_version_df.show()
```

#### 2. Query the table at a specific version to view the older records
```python
# Query table at a specific version
specific_version_df = spark.read.format("delta").option("versionAsOf", specific_version).load("/path/to/delta/employees")
specific_version_df.show()
```

### Task 6: Vacuum Delta Table

#### 1. Use the vacuum operation on the employees Delta table to remove old versions
```python
# Vacuum the Delta table with a retention period of 7 days
delta_employees.vacuum(retentionHours=168)
```

#### 2. Set the retention period to 7 days and ensure that old files are deleted
```python
# Check the files before and after vacuuming to verify the retention period
```
