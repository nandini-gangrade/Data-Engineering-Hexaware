from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Advanced DataFrame Operations - Different Dataset") \
    .getOrCreate()

# Create two sample DataFrames for Product Sales
data1 = [
    (1, 'Product A', 'Electronics', 1200, '2022-05-10'),
    (2, 'Product B', 'Clothing', 500, '2022-07-15'),
    (3, 'Product C', 'Electronics', 1800, '2021-11-05')
]

data2 = [
    (4, 'Product D', 'Furniture', 3000, '2022-03-25'),
    (5, 'Product E', 'Clothing', 800, '2022-09-12'),
    (6, 'Product F', 'Electronics', 1500, '2021-10-19')
]

# Define schema (columns)
columns = ['ProductID', 'ProductName', 'Category', 'Price', 'SaleDate']

# Create DataFrames
sales_df1 = spark.createDataFrame(data1, columns)
sales_df2 = spark.createDataFrame(data2, columns)

# 1. Union of DataFrames (removing duplicates)
combined_df = sales_df1.union(sales_df2).distinct()
print("Combined DataFrame (without duplicates):")
combined_df.show()

# 2. Union of DataFrames (including duplicates)
combined_df_with_duplicates = sales_df1.union(sales_df2)
print("Combined DataFrame (with duplicates):")
combined_df_with_duplicates.show()

# 3. Rank products by price within their category
window_spec = Window.partitionBy("Category").orderBy(F.desc("Price"))
ranked_df = combined_df.withColumn("Rank", F.rank().over(window_spec))
print("Ranked products by price within category:")
ranked_df.show()

# 4. Calculate cumulative price per category
cumulative_price_df = combined_df.withColumn("CumulativePrice", F.sum("Price").over(window_spec))
print("Cumulative price per category:")
cumulative_price_df.show()

# 5. Convert `SaleDate` from string to date type
converted_df = combined_df.withColumn("SaleDate", F.to_date("SaleDate", "yyyy-MM-dd"))
print("DataFrame with SaleDate converted to date type:")
converted_df.show()

# 6. Calculate the number of days since each sale
current_date = F.current_date()
days_since_sale_df = converted_df.withColumn("DaysSinceSale", F.datediff(current_date, "SaleDate"))
print("Number of days since each sale:")
days_since_sale_df.show()

# 7. Add a column for the next sale deadline
next_sale_deadline_df = converted_df.withColumn("NextSaleDeadline", F.date_add("SaleDate", 30))
print("DataFrame with NextSaleDeadline (30 days after SaleDate):")
next_sale_deadline_df.show()

# 8. Calculate total revenue and average price per category
revenue_avg_df = combined_df.groupBy("Category").agg(
    F.sum("Price").alias("TotalRevenue"),
    F.avg("Price").alias("AveragePrice")
)
print("Total revenue and average price per category:")
revenue_avg_df.show()

# 9. Convert all product names to lowercase
lowercase_names_df = combined_df.withColumn("ProductNameLowercase", F.lower("ProductName"))
print("DataFrame with product names in lowercase:")
lowercase_names_df.show()
