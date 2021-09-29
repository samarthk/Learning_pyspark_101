
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

CONSTANTS = 'I am Samarth'
print(type(CONSTANTS))

df = spark.createDataFrame([{"hello": "world"} for x in range(1000)])
df.show(3)
df.show(6)

print(CONSTANTS)

fireServiceCallsDF = spark.read.csv('/home/sammy/Learning_pyspark/DE_101/Fire_Department_Calls_for_Service.csv', header=True, inferSchema=True)
#fireServiceCallsDF.printSchema()
#fireServiceCallsDF.show()

print fireServiceCallsDF.count()

print fireServiceCallsDF.collect()
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

fireServiceCallsDF.createOrReplaceTempView("fireServiceCalls")

spark.sql("SELECT COUNT(*) , Call_Type FROM fireServiceCalls group by Call_Type order by 1 desc").show(1000, False)
