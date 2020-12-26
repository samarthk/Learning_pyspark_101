

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

#"jdbc:oracle:thin:username/password@//hostname:portnumber/SID"

#Check DB ORACLE/

myrdd2 = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:oracle:thin:user2/user2@//192.168.1.151:1521/xe")\
    .option("dbtable", "USER2.DEPT")\
    .option("user", "user2")\
    .option("user", "user2")\
    .option("driver", "oracle.jdbc.OracleDriver").load()

myrdd2.printSchema()
myrdd2.show()
myrdd2.describe()
myrdd2.collect()
myrdd2.count()

print("Connected ORACLE")
print(myrdd2)

print(myrdd2.collect())

myrdd3 = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:oracle:thin:user2/user2@//192.168.1.151:1521/xe")\
    .option("dbtable", "USER2.STG_FireIncidents")\
    .option("user", "user2")\
    .option("user", "user2")\
    .option("driver", "oracle.jdbc.OracleDriver").load()

myrdd3.printSchema()
