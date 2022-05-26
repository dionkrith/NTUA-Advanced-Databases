from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName('rdd-query2').getOrCreate()

sc = spark.sparkContext

start = time.time()

def func(x, y):
	if x[0] > 3 and y[0] > 3:
		return (-1, 2, x[2] + y[2])
	if x[0] > 3:
		if y[0] > 3:
			return (-1, 2, x[2] + y[2])
		elif y[0] != -1:
			return (-1, 1, x[2] + y[2])
		else:
			return (-1, 1 + y[1], x[2] + y[2])
	elif x[0] != -1:
		if y[0] > 3:
			return (-1, 1, x[2] + y[2])
		elif y[0] != -1:
			return (-1, 0, x[2] + y[2])
		else:
			return (-1, y[1], x[2] + y[2])
	else:
		if y[0] > 3:
			return (-1, 1 + x[1], x[2] + y[2])
		elif y[0] != -1:
			return (-1, x[1], x[2] + y[2])
		else:
			return (-1, x[1] + y[1], x[2] + y[2])


rdd = sc.textFile("hdfs://master:9000/files/ratings.csv")


result = rdd.map(lambda x: (x.split(",")[0], (float(x.split(",")[2]), 1))).\
	reduceByKey(lambda x, y :(x[0] + y[0], x[1] + y[1])) .\
        map(lambda x: ( "same key" , (x[1][0]/x[1][1] , 0, 1))) .\
	reduceByKey(func ).\
        map(lambda x: 100*x[1][1]/x[1][2])


result.saveAsTextFile("hdfs://master:9000/outputs/rdd_query2_output.txt")

end = time.time()

print("Execution Time: {} sec".format(end-start))

for i in result.take(1):
	print(i)




