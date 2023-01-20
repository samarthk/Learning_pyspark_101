
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameReader
from pyspark.sql import SQLContext


spark = SparkSession.builder.master("local[*]").getOrCreate()


CONSTANTS = 'I am Samarth'
print(type(CONSTANTS))

sc = spark.sparkContext
sqlContext = SQLContext(sc)

CONSTANTS101 = 'I am Sunny, I am DON'

#df1 = sqlContext.read.format("jdbc").options(url="jdbc:mysql://localhost:3306/sakila",driver="com.mysql.jdbc.Driver",dbtable="actor",user="root",password="****").load()
#mysql_url="jdbc:mysql://localhost:3306/sakila?user=root&password=****"


df1= sqlContext.read.format('jdbc').options(url='jdbc:sqlite:/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db',dbtable='employee',driver='org.sqlite.JDBC').load()



 #   .read.__format__('jdbc').options(url='jdbc:sqlite:/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db',dbtable='employee',driver='org.sqlite.JDBC').load()

df1.printSchema()  #to see your schema.

#print fireServiceCallsDF.count()

df = spark.createDataFrame([{"hello": "world"} for x in range(1000)])
df.show(3)

print(CONSTANTS)

fireServiceCallsDF = spark.read.csv('/home/sammy/Learning_pyspark/DE_101/Fire_Department_Calls_for_Service.csv', header=True, inferSchema=True)
#fireServiceCallsDF.printSchema()
#fireServiceCallsDF.show()

print fireServiceCallsDF.count()


fireServiceCallsDF.collect()
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

fireServiceCallsDF.createOrReplaceTempView("fireServiceCalls")
spark.sql("SELECT COUNT(*) , Call_Type FROM fireServiceCalls group by Call_Type order by 1 desc").show(10, False)
