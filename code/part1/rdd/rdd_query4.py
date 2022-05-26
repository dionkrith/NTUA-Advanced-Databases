from pyspark.sql import SparkSession
from io import StringIO
import csv
import re
import time

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

def find_years(x):
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

def keep_only_words(input_list):
	output_list = [re.sub(r'[^A-Za-z]+', ' ', item) for item in input_list if item!= ""]
	output_list = [item.replace(" ", "") for item in output_list if item.replace(" ", "") !=""]
	return (output_list)


spark = SparkSession.builder.appName('rdd-query4').getOrCreate()

sc = spark.sparkContext

start = time.time()

genres  = sc.textFile("hdfs://master:9000/files/movie_genres.csv").\
	map( lambda x: (x.split(",")[0] , x.split(",")[1])).\
	filter(lambda x: x[1] == 'Drama')

movies = sc.textFile("hdfs://master:9000/files/movies.csv").\
	map(lambda x: (split_complex(x)[0] , (split_complex(x)[2]  , split_complex(x)[3].split("-")[0]))).\
	filter(lambda x : x[1][0] != "" and x[1][1]!= "" and int(x[1][1])>1999).\
	map(lambda x: (x[0] , (len(keep_only_words(x[1][0].split(" "))) , find_years(int(x[1][1])))))
	

result = genres.join(movies).\
	map(lambda x: (x[1][1][1] , (x[1][1][0] , 1))).\
	reduceByKey(lambda x , y : (x[0]+ y[0] , x[1] + y[1] )).\
	map(lambda x: (x[0] , x[1][0]/x[1][1])).\
	sortByKey()

result.saveAsTextFile("hdfs://master:9000/outputs/rdd_query4_output.txt")
end = time.time()

print("Execution time = {} sec".format(end - start))

for i in result.take(10):
	print(i)

