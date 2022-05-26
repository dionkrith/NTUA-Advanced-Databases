from pyspark.sql import SparkSession
from pyspark import RDD
from io import StringIO
import csv
import sys
import time

spark = SparkSession.builder.appName('repartitionjoin').getOrCreate()
sc = spark.sparkContext


def join_fun(x):
        R = []
        L = []

        for item in x:
                if (item[1]== "R"):
                        R.append(item[0])
                else:
                        L.append(item[0])

        return_list = []
        for item_L in L:
                for item_R in R:
                        return_list.append((item_L , item_R))

        return return_list


def repartition_join(self, R):
	L = self.map(lambda x: (x.pop(join_key_1) , (x , "L")))
	R = R.map(lambda x: (x.pop(join_key_2) , (x , "R")))
	output = L.union(R).\
        	groupByKey().\
        	flatMapValues(join_fun).\
        	map(lambda x: (x[0] , (tuple(x[1][0]) , tuple(x[1][1]))) if (len(x[1][0])>1 and len(x[1][1])>1) else (x[0] , (str(x[1][0][0]) , tuple(x[1][1]))) if (len(x[1][0])==1 and   len(x[1][1])>1) else  (x[0] , (tuple(x[1][0]) , str(x[1][1][0]))) if (len(x[1][1])==1 and  len(x[1][0])>1) else (x[0] , (str(x[1][0][0]) , str(x[1][1][0]))))
	return output
		

if len(sys.argv)!=5:
        print("You must give two input csv files with the index of their join key.")
        sys.exit(100)


file_1 = sys.argv[1]
join_key_1 = int(sys.argv[2])

file_2 = sys.argv[3]
join_key_2 = int(sys.argv[4])

RDD.repartition_join = repartition_join

start = time.time()

L  = sc.textFile(file_1).\
	map(lambda x: (x.split(",")))

R  =  sc.textFile(file_2).\
	map(lambda x: (x.split(",")))

rdd = L.repartition_join(R)

rdd.saveAsTextFile("hdfs://master:9000/outputs/repartitionjoin_output.txt")

end = time.time()

print("Execution time = {} sec".format(end - start))

"""
for i in rdd.take(10):
	print(i)
"""
