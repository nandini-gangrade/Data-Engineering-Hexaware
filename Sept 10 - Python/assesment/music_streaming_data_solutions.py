# Music Streaming Data Solutions

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, col, count, when

# Initialize Spark session
spark = SparkSession.builder.appName('MusicStreamingData').getOrCreate()

# Load the dataset
data = spark.read.csv("/content/music_streaming_data.csv", header=True, inferSchema=True)

# Exercise 1
# 1. Calculate the Total Listening Time for Each User
# Group the data by user_id and calculate the total time spent streaming (in seconds) for each user.
total_listening_time_per_user = data.groupBy("user_id").agg(sum("duration_seconds").alias("total_listening_time"))
print("Total listening time for each user:")
total_listening_time_per_user.show()

# Exercise 2
# 2. Filter Songs Streamed for More Than 200 Seconds
# Filter the dataset to show only the songs where the duration_seconds is greater than 200.
songs_gt_200_seconds = data.filter(data["duration_seconds"] > 200)
print("Songs streamed for more than 200 seconds:")
songs_gt_200_seconds.show()

# Exercise 3
# 3. Find the Most Popular Artist (by Total Streams)
# Group the data by artist and find the artist with the most streams (i.e., the highest number of song plays).
most_popular_artist = data.groupBy("artist").agg(count("song_title").alias("total_streams")) \
    .orderBy(col("total_streams").desc()).limit(1)
print("Most popular artist by total streams:")
most_popular_artist.show()

# Exercise 4
# 4. Identify the Song with the Longest Duration
# Identify the song with the longest duration in the dataset.
longest_duration_song = data.orderBy(col("duration_seconds").desc()).limit(1)
print("Song with the longest duration:")
longest_duration_song.show()

# Exercise 5
# 5. Calculate the Average Song Duration by Artist
# Group the data by artist and calculate the average song duration for each artist.
avg_song_duration_per_artist = data.groupBy("artist").agg(avg("duration_seconds").alias("avg_duration"))
print("Average song duration by artist:")
avg_song_duration_per_artist.show()

# Exercise 6
# 6. Find the Top 3 Most Streamed Songs per User
# For each user, find the top 3 most-streamed songs (i.e., songs they played most frequently).
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("user_id").orderBy(col("duration_seconds").desc())
top_songs_per_user = data.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 3)
print("Top 3 most-streamed songs per user:")
top_songs_per_user.show()

# Exercise 7
# 7. Calculate the Total Number of Streams per Day
# Group the data by streaming_time (by extracting the date) and calculate the total number of streams for each day.
from pyspark.sql.functions import to_date

data_with_date = data.withColumn("date", to_date(col("streaming_time")))
total_streams_per_day = data_with_date.groupBy("date").agg(count("song_title").alias("total_streams"))
print("Total number of streams per day:")
total_streams_per_day.show()

# Exercise 8
# 8. Identify Users Who Streamed Songs from More Than One Artist
# Find users who listened to songs by more than one artist.
distinct_artists_per_user = data.groupBy("user_id").agg(count("artist").alias("distinct_artists"))
users_multiple_artists = distinct_artists_per_user.filter(distinct_artists_per_user["distinct_artists"] > 1)
print("Users who streamed songs from more than one artist:")
users_multiple_artists.show()

# Exercise 9
# 9. Calculate the Total Streams for Each Location
# Group the data by location and calculate the total number of streams for each location.
total_streams_per_location = data.groupBy("location").agg(count("song_title").alias("total_streams"))
print("Total streams for each location:")
total_streams_per_location.show()

# Exercise 10
# 10. Create a New Column to Classify Long and Short Songs
# Add a new column song_length that classifies a song as "Long" if duration_seconds > 200, otherwise classify it as "Short."
data = data.withColumn("song_length", when(data["duration_seconds"] > 200, "Long").otherwise("Short"))
print("New column to classify song length:")
data.show()
