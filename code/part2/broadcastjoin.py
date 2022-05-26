from pyspark.sql import SparkSession
from pyspark import RDD
from io import StringIO
import csv
import sys
import time

spark = SparkSession.builder.appName('broadcastjoin').getOrCreate()
sc = spark.sparkContext


def broadcast_join(self, R):
    hash_table = sc.broadcast(R.map(lambda r: (r[0], r)).groupByKey().collectAsMap())
    rdd = self.flatMap(lambda x: ((x[0],(r[1], tuple(x[1]))) for r in hash_table.value.get(x[0], ())))
    return rdd
			

if len(sys.argv)!=5:
        print("You must give two input csv files with the index of their join key.")
        sys.exit(100)


file_1 = sys.argv[1]
join_key_1 = int(sys.argv[2])

file_2 = sys.argv[3]
join_key_2 = int(sys.argv[4])


RDD.broadcast_join = broadcast_join

start = time.time()

R  = sc.textFile(file_1).\
        map(lambda x: (x.split(",")))


L  =  sc.textFile(file_2).\
        map(lambda x: (x.split(","))).\
        map(lambda x: (x.pop(join_key_2) , x))


rdd = L.broadcast_join(R)

rdd.saveAsTextFile("hdfs://master:9000/outputs/broadcastjoin_output.txt")

"""
for i in rdd.take(10):
	print(i)
"""

end = time.time()

print("Execution time = {} sec".format(end - start))


