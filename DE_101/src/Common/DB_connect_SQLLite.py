

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

#"jdbc:oracle:thin:username/password@//hostname:portnumber/SID"

#myrdd1 = spark.read.format("jdbc").options(url="jdbc:oracle:thin:user2/user2@//192.168.1.151:1521/xe ",driver="oracle.jdbc.OracleDriver",dbtable="DEPT").load().take(10)

myrdd1 = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:sqlite:/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db")\
    .option("dbtable", "Album")\
    .option("driver", "org.sqlite.JDBC").load()


myrdd1.printSchema()
myrdd1.show()
myrdd1.describe()
myrdd1.collect()
myrdd1.count()


print("Connected ORACLE")
print(myrdd1)



#Check SQLLite JDBC
print("Hello 101")
myrdd = spark.read.format("jdbc").options(url="jdbc:sqlite:/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db",driver="org.sqlite.JDBC",dbtable="Album").load()
print("Connected 101")
print(myrdd.count())

print(myrdd.collect())

