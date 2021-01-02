from pyspark.sql import SparkSession
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#spark = SparkSession.builder.master("local[3]").getOrCreate()



#Extract
fireIncidentsDF = spark.read.load("/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv",format="csv", sep=",", inferSchema="true", header="true")
fireCallDF = spark.read.load("/home/sammy/Learning_pyspark/InDir/Fire_Department_Calls_for_Service.csv",format="csv", sep=",", inferSchema="true", header="true")

#fireCallDF.printSchema()
#fireIncidentsDF.printSchema()

#fireIncidentsDF.sort("Incident_Date").show(6)



changeFormatDF = fireIncidentsDF.withColumn('Incident_Date', F.to_timestamp('Incident_Date', 'MM/dd/yyyy'))\
    .withColumn('Alarm_DtTm', F.to_timestamp('Alarm_DtTm','MM/dd/yyyy hh:mm:ss aa'))\
    .withColumn('Arrival_DtTm', F.to_timestamp('Arrival_DtTm','MM/dd/yyyy hh:mm:ss aa'))\
    .withColumn('Close_DtTm', F.to_timestamp('Close_DtTm','MM/dd/yyyy hh:mm:ss aa'))\
    .withColumn('City',F.upper(fireIncidentsDF.City))

handleNullDF= changeFormatDF.fillna({'City':'NA'}).fillna({'Arrival_DtTm':'1900-01-01 00:00:00'}).fillna({'Estimated_Property_Loss':0})

filterDF= handleNullDF.filter(handleNullDF.Incident_Date>='2018-01-01 00:00:00')


'''
filterDF= filterDF.select('Exposure_Number','ID','Address','Incident_Date','Call_Number','Alarm_DtTm')
filterDF.show(2)
'''

#handleNullDF.groupby(['City']).agg(F.sum('Estimated_Property_Loss').alias("Total"), F.max("Estimated_Property_Loss").alias("Max")).sort(F.desc("Max")).show(10)

#handleNullDF.sort(F.desc("Incident_Date")).show(6)


handleNullDF.registerTempTable('fireIncidentsTT')

fireCallDF.registerTempTable('fireCallTT')

spark.sql("select City, count(1) as totals "
                  "from fireIncidentsTT "
                  "group by City having count(1) >1  order by 2 desc").show()

'''spark.sql("select City, count(1) as totals "
                  "from fireCallTT "
                  "group by City having count(1) >1  order by 2 ").show()
'''

#spark.sql("select * from  fireIncidentsTT a inner join fireCallTT b on a.Incident_Number= b.Incident_Number ").show()

joinDF = handleNullDF.join(broadcast(fireCallDF), ['Incident_Number'], how='inner').show()


rnkDF=spark.sql(" select City, totals , rank() over( ORDER BY totals desc) as rnk from  "
          "(select City, count(1) as totals from fireIncidentsTT group by City having count(1) >1 ) ")

rnkDF.show(8)

#pivotedCityDF = rnkDF.groupBy('rnk').pivot('City').agg(F.sum('totals').alias('totals_pvt') )

pivotedCityDF = rnkDF.groupBy('rnk').pivot('City').agg(F.sum('totals').alias('confirmed'))

pivotedCityDF.show()

newColnames = [x for x in pivotedCityDF.columns]
print newColnames

filterDF_Part = filterDF.repartition(100)

# filterDF_Part.describe()
#
# print(filterDF_Part.rdd.getNumPartitions())
#
# print filterDF_Part.rdd.glom().collect()
# print("Partitions structure: {}".format(filterDF_Part.rdd.glom().collect()))

filterDF.count()

#Load
try:
    out_DF = filterDF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",table="STG_FireIncidents",mode="append")

except Exception as e:
        print (" ### Exception ###: ", e)
#out_DF = fireIncidentsDF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",isolationLevel=1,table="STG_FireIncidents",mode="append")

time.sleep(5)
