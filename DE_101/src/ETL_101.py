
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()


#Extract

fireIncidentsDF = spark.read.csv('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv', header=True, inferSchema=True)
#fireIncidentsDF.printSchema()
#fireIncidentsDF.show(5)
print(fireIncidentsDF.count())

#filter_DF = fireIncidentsDF.filter(fireIncidentsDF.Incident_Number ==18066225 )

#Transform
sel_DF=fireIncidentsDF.select("Incident_Number", "Exposure_Number", "ID", "Address", "Incident_Date", "Call_Number", "Alarm_DtTm", "Arrival_DtTm", "Close_DtTm", "City", "ZIP_Code", "Battalion", "Station_Area", "Box", "Suppression_Units", "Suppression_Personnel", "EMS_Units", "EMS_Personnel", "Other_Units", "Other_Personnel", "First_Unit_On_Scene", "Estimated_Property_Loss", "Estimated_Contents_Loss", "Fire_Fatalities", "Fire_Injuries").\
    filter(fireIncidentsDF.Incident_Number >=18066225)

print(sel_DF.count())

#Load
try:
    out_DF = sel_DF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",table="STG_FireIncidents",mode="append")

except Exception as e:
        print (" ### Exception ###: ", e)

#out_DF = fireIncidentsDF.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",isolationLevel=1,table="STG_FireIncidents",mode="append")

#print('Done !!')


#print(out_DF)

sel_DF.write.jdbc(url ="jdbc:oracle:thin:user2/user2@//192.168.1.151:1521/xe",table="USER2.STG_FireIncidents",mode="append")

