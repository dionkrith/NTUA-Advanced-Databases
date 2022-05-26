from pyspark.sql import SparkSession
import sys
import time


spark = SparkSession.builder.appName('sparksql-query3').getOrCreate()

if len(sys.argv) == 1:
        print("You must declare the file type.")
        sys.exit(100)

if len(sys.argv)>2:
        print("You must declare ONLY the file type.")
        sys.exit(100)


file_type = sys.argv[1]

start = time.time()

if file_type == 'csv':

	ratings = spark.read.format("csv").\
                options(header = "false" , inferSchema = "true").\
                load("hdfs://master:9000/files/ratings.csv")

	genres = spark.read.format("csv").\
                options(header = "false" , inferSchema = "true").\
                load("hdfs://master:9000/files/movie_genres.csv")

elif file_type == 'parquet':

	ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

	genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")

else:
        print("This file type is not supported")
        sys.exit(100)

"""
+---+-----+---+----------+
|_c0|  _c1|_c2|       _c3|
+---+-----+---+----------+
|  1|  110|1.0|1425941529|
|  1|  147|4.5|1425942435|
|  1|  858|5.0|1425941523|
|  1| 1221|5.0|1425941546|
|  1| 1246|5.0|1425941556|
|  1| 1968|4.0|1425942148|
|  1| 2762|4.5|1425941300|
|  1| 2918|5.0|1425941593|
|  1| 2959|4.0|1425941601|
|  1| 4226|4.0|1425942228|
|  1| 4878|5.0|1425941434|
|  1| 5577|5.0|1425941397|
|  1|33794|4.0|1425942005|
|  1|54503|3.5|1425941313|
|  1|58559|4.0|1425942007|
|  1|59315|5.0|1425941502|
|  1|68358|5.0|1425941464|
|  1|69844|5.0|1425942139|
|  1|73017|5.0|1425942699|
|  1|81834|5.0|1425942133|
+---+-----+---+----------+
"""

ratings = ratings.withColumnRenamed("_c0" , "user_id")
ratings = ratings.withColumnRenamed("_c1" , "movie_id")
ratings = ratings.withColumnRenamed("_c2" , "rating")
ratings = ratings.withColumnRenamed("_c3" , "timestamp")



"""
+-----+---------+
|  _c0|      _c1|
+-----+---------+
|  862|Animation|
|  862|   Comedy|
|  862|   Family|
| 8844|Adventure|
| 8844|  Fantasy|
| 8844|   Family|
|15602|  Romance|
|15602|   Comedy|
|31357|   Comedy|
|31357|    Drama|
|31357|  Romance|
|11862|   Comedy|
|  949|   Action|
|  949|    Crime|
|  949|    Drama|
|  949| Thriller|
|11860|   Comedy|
|11860|  Romance|
|45325|   Action|
|45325|Adventure|
+-----+---------+
"""

genres = genres.withColumnRenamed("_c0" , "movie_id")
genres = genres.withColumnRenamed("_c1" , "category")


ratings.registerTempTable("ratings")
genres.registerTempTable("genres")


QUERY = "select category , avg(avg_rating) as mean_category_rating , count(R.movie_id) as number_of_movies from\
	(select movie_id , avg(rating) as avg_rating from ratings group by movie_id) as R , genres\
	where R.movie_id = genres.movie_id group by category\
	order by category"
	

avg_rating_for_each_movie = spark.sql(QUERY)

if file_type == 'csv':
       avg_rating_for_each_movie.write.csv("hdfs://master:9000/outputs/sparksql-query3_output.csv")

elif file_type == 'parquet':
        avg_rating_for_each_movie.write.parquet("hdfs://master:9000/outputs/sparksql-query3_output.parquet")

else:
        print("That can't be happening.....")

end = time.time()

print("Execution Time = {} sec".format(end - start))

avg_rating_for_each_movie.show()


