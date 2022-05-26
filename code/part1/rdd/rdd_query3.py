from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName('rdd-query3').getOrCreate()
sc = spark.sparkContext

start = time.time()

mean_rating_value_of_each_movie  = sc.textFile("hdfs://master:9000/files/ratings.csv").\
	map(lambda x: (x.split(",")[1] , (float(x.split(",")[2]) , 1))).\
	reduceByKey(lambda x , y : (x[0] + y[0] , x[1] + y[1])).\
	map(lambda x: (x[0] ,x[1][0] / x[1][1] ))

genres = sc.textFile("hdfs://master:9000/files/movie_genres.csv").\
	map(lambda x: (x.split(",")[0] , x.split(",")[1]))


result = mean_rating_value_of_each_movie.join(genres).\
	map(lambda x: (x[1][1] , (x[1][0],1))).\
	reduceByKey(lambda x,y : (x[0] + y[0] , x[1] + y[1])).\
	map(lambda x: (x[0] , x[1][0]/x[1][1] , x[1][1] )).\
	sortBy(lambda x: x[0])

result.saveAsTextFile("hdfs://master:9000/outputs/rdd_query3_output.txt")

end = time.time()

print("Execution time = {} sec".format(end - start))

for i in result.take(20):
	print(i)
