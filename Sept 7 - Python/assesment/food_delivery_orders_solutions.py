# 3. Food Delivery Orders Solutions

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max

# Initialize Spark session
spark = SparkSession.builder.appName('FoodDelivery').getOrCreate()

# Load the data
data = spark.read.csv("/content/food_delivery_orders.csv", header=True, inferSchema=True)

# Exercise 1
# Calculate Total Revenue per Restaurant
# Group the data by restaurant_name and calculate the total revenue for each restaurant.
total_revenue_per_restaurant = data.groupBy("restaurant_name").agg(sum(data["price"] * data["quantity"]).alias("total_revenue"))
print("Total revenue per restaurant")
total_revenue_per_restaurant.show()

# Exercise 2
# Find the Fastest Delivery
# Identify the order with the fastest delivery time.
fastest_delivery = data.orderBy("delivery_time_mins").limit(1)
print("Fastest delivery order")
fastest_delivery.show()

# Exercise 3
# Calculate Average Delivery Time per Restaurant
# Group the data by restaurant_name and calculate the average delivery time for each restaurant.
avg_delivery_time = data.groupBy("restaurant_name").agg(avg("delivery_time_mins").alias("avg_delivery_time"))
print("Average delivery time per restaurant")
avg_delivery_time.show()

# Exercise 4
# Filter Orders for a Specific Customer
# Filter the dataset to include only orders placed by a specific customer (e.g., customer_id = 201).
customer_orders = data.filter(data["customer_id"] == 201)
print("Orders for customer 201")
customer_orders.show()

# Exercise 5
# Find Orders Where Total Amount Spent is Greater Than $20
# Filter orders where the total amount spent (price * quantity) is greater than $20.
high_value_orders = data.filter((data["price"] * data["quantity"]) > 20)
print("Orders where total amount spent is greater than $20")
high_value_orders.show()

# Exercise 6
# Calculate the Total Quantity of Each Food Item Sold
# Group the data by food_item and calculate the total quantity of each food item sold.
total_quantity_per_item = data.groupBy("food_item").agg(sum("quantity").alias("total_quantity"))
print("Total quantity sold for each food item")
total_quantity_per_item.show()

# Exercise 7
# Find the Top 3 Most Popular Restaurants by Number of Orders
# Identify the top 3 restaurants with the highest number of orders placed.
top_3_restaurants = data.groupBy("restaurant_name").count().orderBy("count", ascending=False).limit(3)
print("Top 3 most popular restaurants by number of orders")
top_3_restaurants.show()

# Exercise 8
# Calculate Total Revenue per Day
# Group the data by order_date and calculate the total revenue for each day.
from pyspark.sql.functions import sum
total_revenue_per_day = data.groupBy("order_date").agg(sum(data["price"] * data["quantity"]).alias("total_revenue"))
print("Total revenue per day")
total_revenue_per_day.show()

# Exercise 9
# Find the Longest Delivery Time for Each Restaurant
# For each restaurant, find the longest delivery time.
longest_delivery_time = data.groupBy("restaurant_name").agg(max("delivery_time_mins").alias("max_delivery_time"))
print("Longest delivery time per restaurant")
longest_delivery_time.show()

# Exercise 10
# Create a New Column for Total Order Value
# Add a new column total_order_value that calculates the total value of each order (price * quantity).
data = data.withColumn("total_order_value", data["price"] * data["quantity"])
print("New column for total order value")
data.show()
