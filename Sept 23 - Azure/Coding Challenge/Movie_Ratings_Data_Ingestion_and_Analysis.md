# Movie Ratings Data Ingestion and Analysis

## CSV Data
```csv
UserID,MovieID,Rating,Timestamp
U001,M001,4,2024-05-01 14:30:00
U002,M002,5,2024-05-01 16:00:00
U003,M001,3,2024-05-02 10:15:00
U001,M003,2,2024-05-02 13:45:00
U004,M002,4,2024-05-03 18:30:00
```

## Task 1: Movie Ratings Data Ingestion
Ingest the CSV data into a Delta table in Databricks. Ensure proper error handling for missing or inconsistent data, and log errors accordingly.

```python
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MovieRatings") \
    .getOrCreate()

# Define the CSV file path
file_path = "/path/to/movie_ratings.csv"

# Try to read the CSV file into a DataFrame
try:
    df = spark.read.format("csv").option("header", "true").load(file_path)
    df.write.format("delta").mode("overwrite").save("/delta/movie_ratings")
except AnalysisException as e:
    print(f"Error: {e}")
    # Log the error to a file or monitoring system
```

## Task 2: Data Cleaning
Clean the movie ratings data to ensure that the `Rating` column contains values between 1 and 5, and remove duplicates based on `UserID` and `MovieID`. Save the cleaned data to a new Delta table.

```python
# Load the Delta table
df = spark.read.format("delta").load("/delta/movie_ratings")

# Data cleaning
cleaned_df = df.filter((df.Rating >= 1) & (df.Rating <= 5)) \
                .dropDuplicates(["UserID", "MovieID"])

# Save cleaned data to a new Delta table
cleaned_df.write.format("delta").mode("overwrite").save("/delta/cleaned_movie_ratings")
```

## Task 3: Movie Rating Analysis
Analyze the movie ratings to calculate the average rating for each movie and identify movies with the highest and lowest average ratings. Save the analysis results to a Delta table.

```python
# Load the cleaned Delta table
cleaned_df = spark.read.format("delta").load("/delta/cleaned_movie_ratings")

# Calculate average rating per movie
average_rating_df = cleaned_df.groupBy("MovieID").agg({"Rating": "avg"}).withColumnRenamed("avg(Rating)", "AverageRating")

# Identify movies with the highest and lowest average ratings
highest_rating = average_rating_df.orderBy("AverageRating", ascending=False).first()
lowest_rating = average_rating_df.orderBy("AverageRating").first()

# Save analysis results to a Delta table
average_rating_df.write.format("delta").mode("overwrite").save("/delta/movie_rating_analysis")
```

## Task 4: Time Travel and Delta Lake History
Implement Delta Lake's time travel feature by performing an update to the movie ratings data, rolling back to a previous version to retrieve the original ratings, and using `DESCRIBE HISTORY` to view the changes.

```python
# Load the original Delta table
df = spark.read.format("delta").load("/delta/movie_ratings")

# Update some ratings
updated_df = df.withColumn("Rating", when(df.UserID == "U001", 5).otherwise(df.Rating))
updated_df.write.format("delta").mode("overwrite").save("/delta/movie_ratings")

# Roll back to the previous version
df_rollback = spark.read.format("delta").option("versionAsOf", 0).load("/delta/movie_ratings")

# Use DESCRIBE HISTORY to check changes
history_df = spark.sql("DESCRIBE HISTORY delta.`/delta/movie_ratings`")
history_df.show()
```

## Task 5: Optimize Delta Table
Apply optimizations to the Delta table, implementing Z-ordering on the `MovieID` column, compacting the data, and cleaning up older versions.

```python
# Optimize the Delta table with Z-ordering
spark.sql("OPTIMIZE delta.`/delta/movie_ratings` ZORDER BY (MovieID)")

# Use VACUUM to clean up old data
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/delta/movie_ratings")
deltaTable.vacuum(retentionHours=168)  # Retain data for a week
```
