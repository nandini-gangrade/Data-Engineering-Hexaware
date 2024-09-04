from pyspark.sql import SparkSession

# Step 1: Initialize Spark Context

# Initialize SparkSession and SparkContext
spark = SparkSession.builder \
    .appName("Key-Value Pair RDDs") \
    .getOrCreate()
sc = spark.sparkContext
print("SparkSession created successfully")

# Step 2: Create and Explore the RDD

# Task 1: Create an RDD from the Sales Data
sales_data = [
    ("ProductA", 100),
    ("ProductB", 150),
    ("ProductA", 200),
    ("ProductC", 300),
    ("ProductB", 250),
    ("ProductC", 100)
]

sales_rdd = sc.parallelize(sales_data)
print("Sales RDD: ")
print(sales_rdd.take(5))

# Step 3: Grouping and Aggregating Data

# Task 2: Group Data by Product Name
grouped_data = sales_rdd.groupByKey()
grouped_sales = grouped_data.mapValues(list)
print("Data by product name: ")
print(grouped_sales.collect())

# Task 3: Calculate Total Sales by Product
total_sales = sales_rdd.reduceByKey(lambda x, y: x + y)
print("Total sales by product: ")
print(total_sales.collect())

# Task 4: Sort Products by Total Sales
sorted_sales = total_sales.sortBy(lambda x: x[1], ascending=False)
print("Products by total sales after sorting: ")
print(sorted_sales.collect())

# Step 4: Additional Transformations

# Task 5: Filter Products with High Sales
high_sales = total_sales.filter(lambda x: x[1] > 200)
print("Products with high sales: ")
print(high_sales.collect())

# Task 6: Combine Regional Sales Data
regional_sales_data = [
    ("ProductA", 50),
    ("ProductC", 150)
]

regional_sales_rdd = sc.parallelize(regional_sales_data)
combined_sales = sales_rdd.union(regional_sales_rdd)
new_total_sales = combined_sales.reduceByKey(lambda x, y: x + y)
print("Combined sales data: ")
print(new_total_sales.collect())

# Step 5: Perform Actions on the RDD

# Task 7: Count the Number of Distinct Products
distinct_products = sales_rdd.keys().distinct().count()
print("Number of distinct products: ")
print(distinct_products)

# Task 8: Identify the Product with Maximum Sales
max_sales_product = new_total_sales.reduce(lambda a, b: a if a[1] > b[1] else b)
print("Product with maximum sales: ")
print(max_sales_product)

# Challenge Task: Calculate the Average Sales per Product

# Calculate average sales per product
product_sales_count = combined_sales.mapValues(lambda x: (x, 1))
product_sales_sum_count = product_sales_count.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
average_sales_rdd = product_sales_sum_count.mapValues(lambda x: x[0] / x[1])
print("Average sales per product: ")
print(average_sales_rdd.collect())
