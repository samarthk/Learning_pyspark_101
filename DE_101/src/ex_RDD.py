from pyspark.sql import SparkSession
from pyspark import SparkContext
# Import data types
from pyspark.sql.types import *

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

#print aRDD

incRDD = sc.textFile('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv')
ext = incRDD.take(4)
'''print ext

type(incRDD)
print incRDD.count()
'''
print aRDD.getNumPartitions()

# Load a text file and convert each line to a Row.
lines = sc.textFile('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv')
parts = lines.map(lambda l: l.split(","))
# Each line is converted to a tuple.
people = parts.map(lambda p: (p[0], p[1].strip(), p[2], p[3]))

# The schema is encoded in a string.
schemaString = "name age col1 col2"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = spark.createDataFrame(people, schema)

# Creates a temporary view using the DataFrame
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT * FROM people")

results.show(truncate=False)