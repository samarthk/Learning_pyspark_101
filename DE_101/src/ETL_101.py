
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()



fireIncidentsDF = spark.read.csv('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv', header=True, inferSchema=True)
fireIncidentsDF.printSchema()
fireIncidentsDF.show(5)

fireIncidentsDF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",table="STG_FireIncidents",mode="append")

print('Done !!')