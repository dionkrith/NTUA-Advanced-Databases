from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]
								
spark = SparkSession.builder.appName('rdd-query5').getOrCreate()

sc = spark.sparkContext

start = time.time()
ratings  = sc.textFile("hdfs://master:9000/files/ratings.csv").\
	map(lambda x: ( x.split(",")[1] , (x.split(",")[0] , float(x.split(",")[2]))))



genres = sc.textFile("hdfs://master:9000/files/movie_genres.csv").\
	 map(lambda x: ( x.split(",")[0] , x.split(",")[1]))

movies = sc.textFile("hdfs://master:9000/files/movies.csv").\
        map(lambda x: (split_complex(x)[0]  , (split_complex(x)[1] ,  float(split_complex(x)[7]))))



most_ratings_for_each_category = ratings.join(genres).\
	map(lambda x: ( ( x[1][0][0] , x[1][1]) , 1)).\
	reduceByKey(lambda x , y : x+y).\
	map(lambda x: (x[0][1] , ([x[0][0]] , x[1]))).\
	reduceByKey(lambda x , y : y if y[1]>x[1] else (x[0]+y[0],x[1]) if y[1] == x[1] else x).\
	map(lambda x: ((x[0] , x[1][1]) , x[1][0])).\
	flatMapValues(lambda x: x ).\
	map(lambda x: ((x[1] , x[0][0]), x[0][1]))

"""
most_ratings_for_each_category = ratings.join(genres).\
        map(lambda x: ( ( x[1][0][0] , x[1][1]) , 1)).\
        reduceByKey(lambda x , y : x+y).\
        map(lambda x: (x[0][1] , (x[0][0] , x[1]))).\
        reduceByKey(lambda x , y : y if y[1]>x[1] else x).\
        map(lambda x: ((x[1][0] , x[0]) , x[1][1]))
"""

"""
temp  = ratings.join(genres).\
	join(movies).\
	map(lambda x: ((x[1][0][0][0] , x[1][0][1]) , (x[1][1][0] , x[1][0][0][1] , x[1][1][1])))
"""

temp = ratings.join(movies).\
       map(lambda x: (x[0], (x[1][0][0], x[1][1][0], x[1][0][1], x[1][1][1]))).\
       join(genres).\
       map(lambda x: ( (x[1][0][0], x[1][1]), (x[1][0][1], x[1][0][2], x[1][0][3])))

max_movie = temp.reduceByKey(lambda x , y : x if (x[1] > y[1] or (x[1] == y[1] and x[2] > y[2]))  else y)

min_movie = temp.reduceByKey(lambda x , y : x if (x[1] < y[1] or (x[1] == y[1] and x[2] > y[2]))  else y)


"""
result = most_ratings_for_each_category.join(max_movie).\
	join(min_movie).\
	map(lambda x: (x[0][1]  , x[0][0] , x[1][0][0] , x[1][0][1][0] , x[1][0][1][1] , x[1][1][0] , x[1][1][1])).\
	sortBy(lambda x: x[0])
"""

result = max_movie.join(min_movie). \
        map(lambda x : (x[0], (x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))). \
	join(most_ratings_for_each_category). \
	map(lambda x : (x[0][1], x[0][0], x[1][1], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])). \
        sortBy(lambda x : x[0])

result.saveAsTextFile("hdfs://master:9000/outputs/rdd_query5_output.txt")

end = time.time()

print("Execution time = {} sec".format(end - start))

"""
for i in result.take(2):
	print(i) 	
"""
