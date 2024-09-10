# Health & Fitness Tracker Data Solutions

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, col, count, when

# Initialize Spark session
spark = SparkSession.builder.appName('HealthFitnessTracker').getOrCreate()

# Load the dataset
data = spark.read.csv("/content/health_fitness_tracker.csv", header=True, inferSchema=True)

# Exercise 1
# 1. Find the Total Steps Taken by Each User
# Group the data by user_id and calculate the total steps taken by each user across all days.
total_steps_per_user = data.groupBy("user_id").agg(sum("steps").alias("total_steps"))
print("Total steps taken by each user:")
total_steps_per_user.show()

# Exercise 2
# 2. Filter Days with More Than 10,000 Steps
# Filter the dataset to show only the days where the user took more than 10,000 steps.
days_with_more_than_10000_steps = data.filter(data["steps"] > 10000)
print("Days with more than 10,000 steps:")
days_with_more_than_10000_steps.show()

# Exercise 3
# 3. Calculate the Average Calories Burned by Workout Type
# Group the data by workout_type and calculate the average calories burned for each workout type.
avg_calories_per_workout = data.groupBy("workout_type").agg(avg("calories_burned").alias("avg_calories"))
print("Average calories burned by workout type:")
avg_calories_per_workout.show()

# Exercise 4
# 4. Identify the Day with the Most Steps for Each User
# For each user, find the day when they took the most steps.
most_steps_per_day_per_user = data.groupBy("user_id", "date").agg(max("steps").alias("max_steps"))
print("Day with the most steps for each user:")
most_steps_per_day_per_user.show()

# Exercise 5
# 5. Find Users Who Burned More Than 600 Calories on Any Day
# Filter the data to show only the users who burned more than 600 calories on any day.
users_burned_more_than_600_calories = data.filter(data["calories_burned"] > 600)
print("Users who burned more than 600 calories on any day:")
users_burned_more_than_600_calories.show()

# Exercise 6
# 6. Calculate the Average Hours of Sleep per User
# Group the data by user_id and calculate the average hours of sleep for each user.
avg_sleep_per_user = data.groupBy("user_id").agg(avg("hours_of_sleep").alias("avg_sleep"))
print("Average hours of sleep per user:")
avg_sleep_per_user.show()

# Exercise 7
# 7. Find the Total Calories Burned per Day
# Group the data by date and calculate the total calories burned by all users combined for each day.
total_calories_per_day = data.groupBy("date").agg(sum("calories_burned").alias("total_calories"))
print("Total calories burned per day:")
total_calories_per_day.show()

# Exercise 8
# 8. Identify Users Who Did Different Types of Workouts
# Identify users who participated in more than one type of workout.
workout_types_per_user = data.groupBy("user_id").agg(count("workout_type").alias("num_workout_types"))
users_multiple_workouts = workout_types_per_user.filter(workout_types_per_user["num_workout_types"] > 1)
print("Users who did different types of workouts:")
users_multiple_workouts.show()

# Exercise 9
# 9. Calculate the Total Number of Workouts per User
# Group the data by user_id and count the total number of workouts completed by each user.
total_workouts_per_user = data.groupBy("user_id").agg(count("workout_type").alias("total_workouts"))
print("Total number of workouts per user:")
total_workouts_per_user.show()

# Exercise 10
# 10. Create a New Column for "Active" Days
# Add a new column called active_day that classifies a day as "Active" if the user took more than 10,000 steps, otherwise classify it as "Inactive."
data = data.withColumn("active_day", when(data["steps"] > 10000, "Active").otherwise("Inactive"))
print("New column for 'Active' days:")
data.show()

