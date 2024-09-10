# 1. Fitness Tracker Solutions

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName('FitnessTracker').getOrCreate()

# Load the data
data = spark.read.csv("/content/fitness_tracker_data.csv", header=True, inferSchema=True)

# Exercise 1
# Find the Total Steps Taken by Each User
# Calculate the total number of steps taken by each user across all days.
total_steps = data.groupBy("user_id").agg(sum("steps").alias("total_steps"))
print("Total steps of each user")
total_steps.show()

# Exercise 2
# Filter Days Where a User Burned More Than 500 Calories
# Identify all days where a user burned more than 500 calories.
burned_500_calories = data.filter(data["calories"] > 500)
print("Days where user burned more than 500 calories")
burned_500_calories.show()

# Exercise 3
# Calculate the Average Distance Traveled by Each User
# Calculate the average distance traveled (distance_km) by each user across all days.
average_distance = data.groupBy("user_id").agg(avg("distance_km").alias("avg_distance"))
print("Average distance traveled by each user")
average_distance.show()

# Exercise 4
# Identify the Day with the Maximum Steps for Each User
# For each user, find the day when they took the maximum number of steps.
max_steps_per_user = data.groupBy("user_id", "date").agg(max("steps").alias("max_steps"))
print("Day with maximum steps")
max_steps_per_user.show()

# Exercise 5
# Find Users Who Were Active for More Than 100 Minutes on Any Day
# Identify users who had active minutes greater than 100 on any day.
active_users = data.filter(data["active_minutes"] > 100)
print("Users who were active for more than 100 minutes on any day")
active_users.show()

# Exercise 6
# Calculate the Total Calories Burned per Day
# Group by date and calculate the total number of calories burned by all users combined for each day.
total_calories_per_day = data.groupBy("date").agg(sum("calories").alias("total_calories"))
print("Total calories burned per day")
total_calories_per_day.show()

# Exercise 7
# Calculate the Average Steps per Day
# Find the average number of steps taken across all users for each day.
average_steps_per_day = data.groupBy("date").agg(avg("steps").alias("avg_steps"))
print("Average steps per day")
average_steps_per_day.show()

# Exercise 8
# Rank Users by Total Distance Traveled
# Rank the users by their total distance traveled, from highest to lowest.
total_distance = data.groupBy("user_id").agg(F.sum("distance_km").alias("total_distance"))
window_spec = Window.orderBy(F.col("total_distance").desc())
ranked_users = total_distance.withColumn("rank", F.rank().over(window_spec))
print("Rank of the users based on distance traveled")
ranked_users.show()

# Exercise 9
# Find the Most Active User by Total Active Minutes
# Identify the user with the highest total active minutes across all days.
most_active_user = data.groupBy("user_id").agg(F.sum("active_minutes").alias("total_active_minutes"))
most_active_user = most_active_user.orderBy(F.col("total_active_minutes").desc())
print("Most active user by total active minutes")
most_active_user.show(1)

# Exercise 10
# Create a New Column for Calories Burned per Kilometer
# Add a new column called calories_per_km that calculates how many calories were burned per kilometer.
data = data.withColumn("calories_per_km", data["calories"] / data["distance_km"])
print("New column for calories burned per kilometer")
data.show()
