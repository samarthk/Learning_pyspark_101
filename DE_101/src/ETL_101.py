
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()



fireIncidentsDF = spark.read.csv('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv', header=True, inferSchema=True)
fireIncidentsDF.printSchema()
fireIncidentsDF.show(5)
print(fireIncidentsDF.count())

filter_DF = fireIncidentsDF.filter(fireIncidentsDF.Incident_Number ==18066225 )

sel_DF=fireIncidentsDF.select("Address")

print(filter_DF.count())

try:
    out_DF = filter_DF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",table="STG_FireIncidents",mode="append")

except Exception as e:
        print (" ### Exception ###: ", e)

#out_DF = fireIncidentsDF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",isolationLevel=1,table="STG_FireIncidents",mode="append")

#print('Done !!')


#print(out_DF)

sel_DF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",table="STG_FireIncidents",mode="append")

