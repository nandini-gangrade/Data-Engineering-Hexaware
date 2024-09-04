New! Keyboard shortcuts … Drive keyboard shortcuts have been updated to give you first-letters navigation
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Product Sales Analysis") \
    .getOrCreate()

# Sample data for products
products = [
    (1, "Laptop", "Electronics", 50000),
    (2, "Smartphone", "Electronics", 30000),
    (3, "Table", "Furniture", 15000),
    (4, "Chair", "Furniture", 5000),
    (5, "Headphones", "Electronics", 2000),
]

# Sample data for sales transactions
sales = [
    (1, 1, 2),
    (2, 2, 1),
    (3, 3, 3),
    (4, 1, 1),
    (5, 4, 5),
    (6, 2, 2),
    (7, 5, 10),
    (8, 3, 1),
]

# Define schema for DataFrames
product_columns = ["ProductID", "ProductName", "Category", "Price"]
sales_columns = ["SaleID", "ProductID", "Quantity"]

# Create DataFrames
product_df = spark.createDataFrame(products, schema=product_columns)
sales_df = spark.createDataFrame(sales, schema=sales_columns)

# Show the DataFrames
print("Products DataFrame:")
product_df.show()

print("Sales DataFrame:")
sales_df.show()

# 1. Join the DataFrames
combined_df = product_df.join(sales_df, on="ProductID")
print("Combined DataFrame:")
combined_df.show()

# 2. Calculate Total Sales Value
total_sales_value_df = combined_df.withColumn("TotalSalesValue", col("Price") * col("Quantity"))
print("Total Sales Value DataFrame:")
total_sales_value_df.show()

# 3. Find the Total Sales for Each Product Category
total_sales_by_category_df = total_sales_value_df.groupBy("Category").agg(_sum("TotalSalesValue").alias("TotalSalesByCategory"))
print("Total Sales by Category DataFrame:")
total_sales_by_category_df.show()

# 4. Identify the Top-Selling Product
top_selling_product_df = total_sales_value_df.groupBy("ProductID", "ProductName").agg(_sum("TotalSalesValue").alias("TotalSalesValue")).orderBy(col("TotalSalesValue").desc()).limit(1)
print("Top-Selling Product:")
top_selling_product_df.show()

# 5. Sort the Products by Total Sales Value
sorted_products_df = total_sales_value_df.groupBy("ProductID", "ProductName").agg(_sum("TotalSalesValue").alias("TotalSalesValue")).orderBy(col("TotalSalesValue").desc())
print("Sorted Products by Total Sales Value:")
sorted_products_df.show()

# 6. Count the Number of Sales for Each Product
sales_count_df = sales_df.groupBy("ProductID").count()
print("Sales Count for Each Product:")
sales_count_df.show()

# 7. Filter the Products with Total Sales Value Greater Than ₹50,000
filtered_products_df = sorted_products_df.filter(col("TotalSalesValue") > 50000)
print("Filtered Products with Total Sales Value Greater Than ₹50,000:")
filtered_products_df.show()
