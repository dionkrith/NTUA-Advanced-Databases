from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName('sparksql-part2_3').getOrCreate()

genres = spark.read.format("csv").\
	options(header = "false" , inferSchema = "true").\
	load("hdfs://master:9000/files/movie_genres.csv")


genres_list = genres.head(100)

genres_100 = spark.createDataFrame(genres_list)

genres_100.write.csv("hdfs://master:9000/files/movie_genres_100.csv")

