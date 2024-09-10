# 4. Weather Data Solutions

!pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count

# Initialize Spark session
spark = SparkSession.builder.appName('WeatherData').getOrCreate()

# Load the data
data = spark.read.csv("/content/weather_data.csv", header=True, inferSchema=True)

# Exercise 1
# Calculate Average Temperature per City
# Group the data by city and calculate the average temperature for each city.
average_temperature = data.groupBy("city").agg(avg("temperature_c").alias("avg_temperature"))
print("Average temperature per city")
average_temperature.show()

# Exercise 2
# Find the Day with the Highest Temperature in Each City
# For each city, find the day with the highest recorded temperature.
highest_temperature = data.groupBy("city", "date").agg(max("temperature_c").alias("max_temperature"))
print("Day with the highest temperature in each city")
highest_temperature.show()

# Exercise 3
# Filter Days with Rainfall Greater Than 20mm
# Identify all days where rainfall was greater than 20mm.
rainy_days = data.filter(data["rain_mm"] > 20)
print("Days with rainfall greater than 20mm")
rainy_days.show()

# Exercise 4
# Calculate the Total Rainfall per City
# Group the data by city and calculate the total amount of rainfall in each city.
total_rainfall = data.groupBy("city").agg(sum("rain_mm").alias("total_rainfall"))
print("Total rainfall per city")
total_rainfall.show()

# Exercise 5
# Find Cities with Average Wind Speed Greater Than 15 km/h
# Filter cities where the average wind speed is greater than 15 km/h.
avg_wind_speed = data.groupBy("city").agg(avg("wind_speed_kmh").alias("avg_wind_speed"))
windy_cities = avg_wind_speed.filter(avg_wind_speed["avg_wind_speed"] > 15)
print("Cities with average wind speed greater than 15 km/h")
windy_cities.show()

# Exercise 6
# Identify the Coldest Day per City
# For each city, find the day with the lowest recorded temperature.
coldest_day = data.groupBy("city", "date").agg(min("temperature_c").alias("min_temperature"))
print("Coldest day in each city")
coldest_day.show()

# Exercise 7
# Calculate Total Snowfall per City
# Group the data by city and calculate the total amount of snowfall.
total_snowfall = data.groupBy("city").agg(sum("snow_mm").alias("total_snowfall"))
print("Total snowfall per city")
total_snowfall.show()

# Exercise 8
# Find the Day with Maximum Wind Speed in Each City
# For each city, find the day with the maximum recorded wind speed.
max_wind_speed = data.groupBy("city", "date").agg(max("wind_speed_kmh").alias("max_wind_speed"))
print("Day with maximum wind speed in each city")
max_wind_speed.show()

# Exercise 9
# Calculate the Number of Days with Temperature Below Freezing
# Count the number of days where the temperature was below 0°C.
freezing_days = data.filter(data["temperature_c"] < 0)
freezing_days_count = freezing_days.groupBy("city").count().alias("freezing_days_count")
print("Number of freezing days per city")
freezing_days_count.show()

# Exercise 10
# Create a New Column for Wind Chill
# Add a new column wind_chill that estimates wind chill based on temperature and wind speed.
# Formula: Wind Chill = 13.12 + 0.6215 * T - 11.37 * W^0.16 + 0.3965 * T * W^0.16
# where T is temperature in °C and W is wind speed in km/h.
from pyspark.sql.functions import expr
data = data.withColumn("wind_chill", expr("13.12 + 0.6215 * temperature_c - 11.37 * pow(wind_speed_kmh, 0.16) + 0.3965 * temperature_c * pow(wind_speed_kmh, 0.16)"))
print("New column for wind chill")
data.show()
