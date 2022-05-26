from pyspark.sql import SparkSession
import sys
import re
import time

def pentaetia(x):
        if(2000<=x<=2004):
                return("2000-2004")
        elif (2005<=x<=2009):
                return("2005-2009")
        elif (2010<=x<=2014):
                return("2010-2014")
        elif (2015<=x<=2019):
                return("2015-2019")
        else:
                print("It can't be hapenning....")
                return 0;

def length(summary):
	input_list = summary.split(" ")
	output_list = [re.sub(r'[^A-Za-z]+', ' ', item) for item in input_list if item!= ""]
	output_list = [item.replace(" ", "") for item in output_list if item.replace(" ", "") !=""]
	return (len(output_list))


spark = SparkSession.builder.appName('sparksql-query4').getOrCreate()

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


elif file_type  == 'parquet':

        genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")

        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")


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


genres.registerTempTable("genres")
movies.registerTempTable("movies")

spark.udf.register("length" , length)
spark.udf.register("pentaetia" , pentaetia)


QUERY = " select pentaetia , avg(length(A.movie_summary)) as average_summary_length from (\
	  select pentaetia(year(movie_date)) as pentaetia , movie_summary from\
	  movies , genres where movies.movie_id = genres.movie_id and genres.category = 'Drama' and year(movie_date) > 1999) as A\
	  where A.movie_summary <>'' group by A.pentaetia order by A.pentaetia"

result = spark.sql(QUERY)

if file_type == 'csv':
        result.write.csv("hdfs://master:9000/outputs/sparksql-query4_output.csv")

elif file_type == 'parquet':
        result.write.parquet("hdfs://master:9000/outputs/sparksql-query4_output.parquet")

else:
        print("That can't be happening.....")

end = time.time()

print("Execution Time = {} sec".format(end - start))

result.show()



