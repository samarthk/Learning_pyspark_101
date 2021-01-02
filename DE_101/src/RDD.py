from pyspark.sql import SparkSession
from pyspark import SparkContext

sc = SparkContext()

import time
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

'''spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
'''
spark = SparkSession.builder.master("local[2]").getOrCreate()

#Extract
fireIncidentsDF = spark.read.load("/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv",format="csv", sep=",", inferSchema="true", header="true")
fireCallDF = spark.read.load("/home/sammy/Learning_pyspark/InDir/Fire_Department_Calls_for_Service.csv",format="csv", sep=",", inferSchema="true", header="true")

#fireCallDF.printSchema()
#fireIncidentsDF.printSchema()

aRDD = fireIncidentsDF.rdd

print aRDD

incRDD = sc.textFile('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv')
ext = incRDD.take(4)
print ext

type(incRDD)
incRDD.count()

print('this is RDD -- ')
print (incRDD)
print aRDD.getNumPartitions()
