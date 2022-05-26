from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('csv_to_parquet').getOrCreate()


movies = spark.read.format("csv").\
                options(header = "false" , InferSchema = "true").\
                load("hdfs://master:9000/files/movies.csv")

movies.write.parquet("hdfs://master:9000/files/movies.parquet")



movie_genres = spark.read.format("csv").\
                options(header = "false" , InferSchema = "true").\
                load("hdfs://master:9000/files/movie_genres.csv")

movie_genres.write.parquet("hdfs://master:9000/files/movie_genres.parquet")

ratings = spark.read.format("csv").\
                options(header = "false" , InferSchema = "true").\
                load("hdfs://master:9000/files/ratings.csv")

ratings.write.parquet("hdfs://master:9000/files/ratings.parquet")



