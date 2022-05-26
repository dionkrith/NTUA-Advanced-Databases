from pyspark.sql import SparkSession
import sys
import time

spark = SparkSession.builder.appName('sparksql-query2').getOrCreate()


if len(sys.argv) ==1:
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

elif file_type == 'parquet':
        ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

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

ratings.registerTempTable("ratings")


QUERY = "select 100*count(*)/(select count(distinct ratings.user_id) from ratings) as Percentage from (select user_id  ,avg(rating) as average_rating from ratings group by user_id) where average_rating > 3"

result = spark.sql(QUERY)

if file_type == 'csv':
       result.write.csv("hdfs://master:9000/outputs/sparksql-query2_output.csv")

elif file_type == 'parquet':
        result.write.parquet("hdfs://master:9000/outputs/sparksql-query2_output.parquet")

else:
        print("That can't be happening.....")

end = time.time()

print("Execution Time = {} sec".format(end - start))

result.show()



