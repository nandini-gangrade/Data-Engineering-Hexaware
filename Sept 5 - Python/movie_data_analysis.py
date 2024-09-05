# movie_data_analysis.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import year

# Initialize Spark session
spark = SparkSession.builder.appName("Movie Data").getOrCreate()

# Load the dataset
movie_df = spark.read.option("header", "true").csv("/content/sample_data/movie_data.csv", inferSchema=True)

# Show the DataFrame
movie_df.show()

# Filter Sci-Fi movies
sci_fi_movies = movie_df.filter(movie_df['genre'] == 'Sci-Fi')
sci_fi_movies.show()

# Get top 3 highest-rated movies
top_rated_movies = movie_df.orderBy(movie_df['rating'], ascending=False).limit(3)
top_rated_movies.show()

# Filter movies released after 2010
movies_after_2010 = movie_df.filter(year(movie_df['date']) > 2010)
movies_after_2010.show()

# Calculate the average box office revenue by genre
avg_box_office_by_genre = movie_df.groupBy("genre").agg(F.avg("box_office").alias("avg_box_office"))
avg_box_office_by_genre.show()

# Add a new column: box office revenue in billions
movie_df = movie_df.withColumn("box_office_in_billions", movie_df['box_office'] / 1e9)
movie_df.show()

# Sort movies by box office revenue
sorted_by_box_office = movie_df.orderBy(movie_df['box_office'], ascending=False)
sorted_by_box_office.show()

# Count the number of movies by genre
movie_count_by_genre = movie_df.groupBy("genre").count()
movie_count_by_genre.show()

# Stop the Spark session
spark.stop()
