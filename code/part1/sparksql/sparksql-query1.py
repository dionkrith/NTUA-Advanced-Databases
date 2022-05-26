from pyspark.sql import SparkSession
import sys
import time


spark = SparkSession.builder.appName('sparksql-query1').getOrCreate()

if len(sys.argv) ==1:
	print("You must declare the file type.")
	sys.exit(100)

if len(sys.argv)>2:
	print("You must declare ONLY the file type.")
	sys.exit(100)


file_type = sys.argv[1]

start = time.time()

if file_type == 'csv': 
	movies = spark.read.format("csv").\
        	options(header = "false" , inferSchema = "true").\
                load("hdfs://master:9000/files/movies.csv")

elif file_type == 'parquet':
	movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")

else:
	print("This file type is not supported")
	sys.exit(100)


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

movies.registerTempTable("movies")


QUERY = "select Table1.movie_year , movies.movie_title , Table1.profit from (select A.movie_year ,max(100*(movie_income - movie_cost)/movie_cost) as profit from (select year(movie_date) as movie_year  , movie_cost , movie_income from movies where year(movies.movie_date) > 1999 and movies.movie_income <>0 and movies.movie_cost <> 0) as A \
	 group by (A.movie_year) order by A.movie_year) as Table1 , movies where Table1.movie_year = year(movies.movie_date) and Table1.profit = 100*(movies.movie_income - movies.movie_cost)/movies.movie_cost order by Table1.movie_year"

result_df = spark.sql(QUERY)

if file_type == 'csv':
	result_df.write.csv("hdfs://master:9000/outputs/sparksql-query1_output.csv")

elif file_type == 'parquet':
	result_df.write.parquet("hdfs://master:9000/outputs/sparksql-query1_output.parquet")

else:
	print("That can't be happening.....")

end = time.time()

print("Execution Time = {} sec".format(end - start))

result_df.show()
