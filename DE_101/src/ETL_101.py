from pyspark.sql import SparkSession
import time

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
#spark = SparkSession.builder.master("local[*]").getOrCreate()

from pyspark.sql.functions import upper, to_timestamp

#Extract
fireIncidentsDF = spark.read.csv('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv', header=True, inferSchema=True)
#fireIncidentsDF.printSchema()

'''Transform
1. Filter Incident_Date for Year later than 2018
2. CamelCase City - Upper Instead
3. Handle All nulls to String - 'NA', Number - 0, Date - '1900-01-01 00:00:00'
'''

changeFormatDF = fireIncidentsDF.withColumn('Incident_Date', to_timestamp('Incident_Date', 'MM/dd/yyyy'))\
    .withColumn('Alarm_DtTm', to_timestamp('Alarm_DtTm','MM/dd/yyyy hh:mm:ss aa'))\
    .withColumn('Arrival_DtTm', to_timestamp('Arrival_DtTm','MM/dd/yyyy hh:mm:ss aa'))\
    .withColumn('Close_DtTm', to_timestamp('Close_DtTm','MM/dd/yyyy hh:mm:ss aa'))\
    .withColumn('City',upper(fireIncidentsDF.City))

handleNullDF= changeFormatDF.fillna({'City':'NA'}).fillna({'Arrival_DtTm':'1900-01-01 00:00:00'}).fillna({'Estimated_Property_Loss':0})
filterDF= handleNullDF.filter(handleNullDF.Incident_Date>='2018-01-01 00:00:00')

filterDF.show(5)
filterDF.count()

#Load
try:
    out_DF = filterDF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",table="STG_FireIncidents",mode="append")

except Exception as e:
        print (" ### Exception ###: ", e)
#out_DF = fireIncidentsDF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",isolationLevel=1,table="STG_FireIncidents",mode="append")

time.sleep(5)
