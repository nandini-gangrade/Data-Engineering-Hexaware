import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Part 1: Dataset Preparation

# Step 1: Generate the Sample Sales Dataset

# Sample sales data
data = {
    "TransactionID": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "CustomerID": [101, 102, 103, 101, 104, 102, 103, 104, 101, 105],
    "ProductID": [501, 502, 501, 503, 504, 502, 503, 504, 501, 505],
    "Quantity": [2, 1, 4, 3, 1, 2, 5, 1, 2, 1],
    "Price": [150.0, 250.0, 150.0, 300.0, 450.0, 250.0, 300.0, 450.0, 150.0, 550.0],
    "Date": [
        datetime(2024, 9, 1),
        datetime(2024, 9, 1),
        datetime(2024, 9, 2),
        datetime(2024, 9, 2),
        datetime(2024, 9, 3),
        datetime(2024, 9, 3),
        datetime(2024, 9, 4),
        datetime(2024, 9, 4),
        datetime(2024, 9, 5),
        datetime(2024, 9, 5)
    ]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
df.to_csv('sales_data.csv', index=False)

print("Sample sales dataset has been created and saved as 'sales_data.csv'.")

# Part 2: Load and Analyze the Dataset Using PySpark

# Step 2: Load the Dataset into PySpark

# 1. Initialize the SparkSession
spark = SparkSession.builder \
    .appName("Sales Dataset Analysis") \
    .getOrCreate()

# 2. Load the CSV File into a PySpark DataFrame
sales_df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)
sales_df.show()

# Step 3: Explore the Data

# 1. Print the Schema
sales_df.printSchema()

# 2. Show the First Few Rows
sales_df.show(5)

# 3. Get Summary Statistics
sales_df.describe(['Quantity', 'Price']).show()

# Step 4: Perform Data Transformations and Analysis

# 1. Calculate the Total Sales Value for Each Transaction
sales_df = sales_df.withColumn("TotalSales", col("Quantity") * col("Price"))
sales_df.show(5)

# 2. Group By ProductID and Calculate Total Sales Per Product
product_sales_df = sales_df.groupBy("ProductID").sum("TotalSales").alias("TotalSales")
product_sales_df.show()

# 3. Identify the Top-Selling Product
top_product = product_sales_df.orderBy(col("sum(TotalSales)").desc()).limit(1)
top_product.show()

# 4. Calculate the Total Sales by Date
daily_sales_df = sales_df.groupBy("Date").sum("TotalSales")
daily_sales_df.show()

# 5. Filter High-Value Transactions
high_sales_df = sales_df.filter(col("TotalSales") > 500)
high_sales_df.show()

# Additional Challenge (Optional)

# 1. Identify Repeat Customers
customer_purchase_count = sales_df.groupBy("CustomerID").count().filter(col("count") > 1)
customer_purchase_count.show()

# 2. Calculate the Average Sale Price Per Product
avg_price_per_unit = sales_df.groupBy("ProductID").avg("Price").alias("AvgPricePerUnit")
avg_price_per_unit.show()
