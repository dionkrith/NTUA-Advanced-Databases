from pyspark.sql import SparkSession
import sys
import time

spark = SparkSession.builder.appName('sparksql-query5').getOrCreate()

if len(sys.argv) ==1:
        print("You must declare the file type.")
        sys.exit(100)

if len(sys.argv)>2:
        print("You must declare ONLY the file type.")
        sys.exit(100)


file_type = sys.argv[1]

start = time.time()

if file_type == 'csv':

	genres = spark.read.format("csv").\
                options(header = "false" , inferSchema = "true").\
                load("hdfs://master:9000/files/movie_genres.csv")

	movies = spark.read.format("csv").\
                options(header = "false" , inferSchema = "true").\
                load("hdfs://master:9000/files/movies.csv")

	ratings = spark.read.format("csv").\
                options(header = "false" , inferSchema = "true").\
                load("hdfs://master:9000/files/ratings.csv")

elif file_type  == 'parquet':

	genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")

	movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")

	ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")



else:
        print("This file type is not supported")
        sys.exit(100)



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


genres = genres.withColumnRenamed("_c0" , "movie_id")
genres = genres.withColumnRenamed("_c1" , "category")



"""
+-----+--------------------+--------------------+-------------------+-----+--------+---------+---------+
|  _c0|                 _c1|                 _c2|                _c3|  _c4|     _c5|      _c6|      _c7|
+-----+--------------------+--------------------+-------------------+-----+--------+---------+---------+
|  862|           Toy Story|Led by Woody Andy...|1995-10-30 00:00:00| 81.0|30000000|373554033|21.946943|
| 8844|             Jumanji|When siblings Jud...|1995-12-15 00:00:00|104.0|65000000|262797249|17.015539|
|15602|    Grumpier Old Men|A family wedding ...|1995-12-22 00:00:00|101.0|       0|        0|  11.7129|
|31357|   Waiting to Exhale|Cheated on mistre...|1995-12-22 00:00:00|127.0|16000000| 81452156| 3.859495|
|11862|Father of the Bri...|Just when George ...|1995-02-10 00:00:00|106.0|       0| 76578911| 8.387519|
|  949|                Heat|Obsessive master ...|1995-12-15 00:00:00|170.0|60000000|187436818|17.924927|
|11860|             Sabrina|An ugly duckling ...|1995-12-15 00:00:00|127.0|58000000|        0| 6.677277|
|45325|        Tom and Huck|A mischievous you...|1995-12-22 00:00:00| 97.0|       0|        0| 2.561161|
| 9091|        Sudden Death|International act...|1995-12-22 00:00:00|106.0|35000000| 64350171|  5.23158|
|  710|           GoldenEye|James Bond must u...|1995-11-16 00:00:00|130.0|58000000|352194034|14.686036|
| 9087|The American Pres...|Widowed US presid...|1995-11-17 00:00:00|106.0|62000000|107879496| 6.318445|
|12110|Dracula: Dead and...|When a lawyer sho...|1995-12-22 00:00:00| 88.0|       0|        0| 5.430331|
|21032|               Balto|An outcast halfwo...|1995-12-22 00:00:00| 78.0|       0| 11348324|12.140733|
|10858|               Nixon|An allstar cast p...|1995-12-22 00:00:00|192.0|44000000| 13681765|    5.092|
| 1408|    Cutthroat Island|Morgan Adams and ...|1995-12-22 00:00:00|119.0|98000000| 10017322| 7.284477|
|  524|              Casino|The life of the g...|1995-11-22 00:00:00|178.0|52000000|116112375|10.137389|
| 4584|Sense and Sensibi...|Rich Mr Dashwood ...|1995-12-13 00:00:00|136.0|16500000|135000000|10.673167|
|    5|          Four Rooms|Its Ted the Bellh...|1995-12-09 00:00:00| 98.0| 4000000|  4300000| 9.026586|
| 9273|Ace Ventura: When...|Summoned from an ...|1995-11-10 00:00:00| 90.0|30000000|212385533| 8.205448|
|11517|         Money Train|A vengeful New Yo...|1995-11-21 00:00:00|103.0|60000000| 35431113| 7.337906|
+-----+--------------------+--------------------+-------------------+-----+--------+---------+---------+
"""

movies = movies.withColumnRenamed("_c0" , "movie_id")
movies = movies.withColumnRenamed("_c1" , "movie_title")
movies = movies.withColumnRenamed("_c2" , "movie_summary")
movies = movies.withColumnRenamed("_c3" , "movie_date")
movies = movies.withColumnRenamed("_c4" , "movie_duratiom")
movies = movies.withColumnRenamed("_c5" , "movie_cost")
movies = movies.withColumnRenamed("_c6" , "movie_income")
movies = movies.withColumnRenamed("_c7" , "movie_popularity")


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


genres.registerTempTable("genres")
movies.registerTempTable("movies")
ratings.registerTempTable("ratings")




QUERY2 = "SELECT TABLE_A.category, TABLE_A.user_id, TABLE_A.max_number_of_ratings_per_category AS total_ratings, TABLE_B.favorite_movie, TABLE_B.favorite_rating, TABLE_C.worst_movie, TABLE_C.worst_rating FROM \
		(SELECT  T1.category , T2.user_id , T1.max_number_of_ratings_per_category FROM \
			 (SELECT category , MAX(number_of_ratings_per_user) AS max_number_of_ratings_per_category FROM \
				 (SELECT category , user_id , count(rating) AS number_of_ratings_per_user FROM \
                	       	 	genres , ratings WHERE genres.movie_id = ratings.movie_id \
				  GROUP BY category , user_id) as A GROUP BY A.category) AS T1, \
			  (SELECT category , user_id , COUNT(rating) as number_of_ratings_per_user FROM \
				 genres, ratings WHERE genres.movie_id = ratings.movie_id GROUP BY genres.category , user_id) AS  T2 \
	  	WHERE T1.category = T2.category AND T1.max_number_of_ratings_per_category = T2.number_of_ratings_per_user ORDER BY T1.category) AS TABLE_A, \
		(SELECT TABLE3.category, TABLE3.user_id, FIRST(TABLE3.movie_title) AS favorite_movie, FIRST(TABLE3.rating) AS favorite_rating FROM \
                	(SELECT TABLE1.category, TABLE1.user_id, TABLE1.movie_title, TABLE1.rating FROM \
                        	(SELECT category, user_id, movies.movie_id, movies.movie_title, rating, movies.movie_popularity FROM \
                               	       genres,ratings, movies WHERE genres.movie_id = ratings.movie_id AND ratings.movie_id = movies.movie_id AND genres.movie_id = movies.movie_id) TABLE1 \
                  	ORDER BY TABLE1.category, TABLE1.user_id, TABLE1.rating DESC, TABLE1.movie_popularity DESC) TABLE3 \
          	GROUP BY category, user_id) TABLE_B , \
		(SELECT TABLE3.category, TABLE3.user_id, FIRST(TABLE3.movie_title) AS worst_movie, FIRST(TABLE3.rating) AS worst_rating FROM \
                        (SELECT TABLE1.category, TABLE1.user_id, TABLE1.movie_title, TABLE1.rating FROM \
                                (SELECT category, user_id, movies.movie_id, movies.movie_title, rating, movies.movie_popularity FROM \
                                       genres,ratings, movies WHERE genres.movie_id = ratings.movie_id AND ratings.movie_id = movies.movie_id AND genres.movie_id = movies.movie_id) TABLE1 \
                        ORDER BY TABLE1.category, TABLE1.user_id, TABLE1.rating ASC, TABLE1.movie_popularity DESC) TABLE3 \
                GROUP BY category, user_id) TABLE_C WHERE TABLE_A.category = TABLE_B.category AND TABLE_A.user_id = TABLE_B.user_id AND TABLE_A.category = TABLE_C.category AND TABLE_A.user_id = TABLE_C.user_id \
	  ORDER BY TABLE_A.category"


  

"""
query3 = "SELECT category, user_id, favorite_movie, favorite_rating, worst_movie, worst_rating FROM \
	  (SELECT TABLE3.category, TABLE3.user_id, FIRST(TABLE3.movie_title) AS favorite_movie, FIRST(TABLE3.rating) AS favorite_rating, LAST(TABLE3.movie_title) AS worst_movie, LAST(TABLE3.rating) AS worst_rating FROM \
		(SELECT TABLE1.category, TABLE1.user_id, TABLE1.movie_title, TABLE1.rating FROM \
			(SELECT category, user_id, movies.movie_id, movies.movie_title, rating, movies.movie_popularity FROM \
				genres,ratings, movies WHERE genres.movie_id = ratings.movie_id AND ratings.movie_id = movies.movie_id AND genres.movie_id = movies.movie_id) TABLE1 \
		  ORDER BY TABLE1.category, TABLE1.user_id, TABLE1.rating DESC, TABLE1.movie_popularity DESC) TABLE3 \
	  GROUP BY category, user_id) TABLE4 \
	 WHERE category = 'Action' AND user_id = 8659"

query3 = "WITH TEMPORARY_TABLE(user_id, movie_id, movie_title, max_rating, popularity) AS \
	  	(SELECT TABLE2.user_id, TABLE2.movie_id, movies.movie_title, TABLE2.max_rating, movies.movie_popularity \
	 	 FROM movies \
	  	 JOIN \
	   		(SELECT TABLE1.user_id AS user_id, ratings.movie_id AS movie_id, TABLE1.max_rating AS max_rating FROM ratings INNER JOIN \
				( SELECT user_id, MAX(rating) AS max_rating \
	  			FROM ratings \
	  			GROUP BY user_id) TABLE1 \
	   		ON ratings.user_id = TABLE1.user_id AND ratings.rating = TABLE1.max_rating) TABLE2 \
	  	ON TABLE2.movie_id = movies.movie_id) \
	  SELECT TABLE3.user_id AS user_id, TABLE4.movie_title AS movie_title, TABLE3.max_rating AS max_rating FROM \
	  TEMPORARY_TABLE AS TABLE4\
	  INNER JOIN \
	  	(SELECT TABLE5.user_id AS user_id, TABLE5.max_rating AS max_rating, MAX(TABLE5.popularity) AS max_popularity FROM \
		 TEMPORARY_TABLE AS TABLE5 GROUP BY TABLE5.user_id, TABLE5.max_rating) TABLE3 \
	  ON TABLE3.user_id = TABLE4.user_id AND TABLE3.max_rating = TABLE4.max_rating AND TABLE3.max_popularity = TABLE4.popularity" 
"""
	  
result = spark.sql(QUERY2)


if file_type == 'csv':
        result.write.csv("hdfs://master:9000/outputs/sparksql-query5_output.csv")

elif file_type == 'parquet':
        result.write.parquet("hdfs://master:9000/outputs/sparksql-query5_output.parquet")

else:
        print("That can't be happening.....")

end = time.time()

print("Execution Time = {} sec".format(end - start))

result.show()





