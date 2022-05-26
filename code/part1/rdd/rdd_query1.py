from pyspark.sql import SparkSession
from io import StringIO
import csv
import time

def split_complex(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

spark = SparkSession.builder.appName('rdd-query1').getOrCreate()

sc = spark.sparkContext

start = time.time()	
	
result = sc.textFile("hdfs://master:9000/files/movies.csv").\
        map(lambda x: (split_complex(x)[3].split("-")[0] , (split_complex(x)[1] ,  int(split_complex(x)[5]) , int(split_complex(x)[6]) ) ) ).\
	filter(lambda x: x[0]!= ""  and x[1][1]!=0 and x[1][2]!=0 and int(x[0])>1999).\
	map(lambda x: (x[0] , (x[1][0] , ( (x[1][2] - x[1][1])/x[1][1] )*100 ) )).\
	reduceByKey(lambda x , y: y if y[1] > x[1] else x).\
	sortByKey().\
	map(lambda x: (x[0] , x[1][0] ,x[1][1]))

result.saveAsTextFile("hdfs://master:9000/outputs/rdd_query1_output.txt")	

end = time.time()	

print("Execution time = {} sec".format(end - start))

for i in result.take(10):
	print(i)	


