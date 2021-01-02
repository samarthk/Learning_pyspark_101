
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

from pyspark.sql import DataFrameWriter

#"jdbc:oracle:thin:username/password@//hostname:portnumber/SID"

#myrdd1 = spark.read.format("jdbc").options(url="jdbc:oracle:thin:user2/user2@//192.168.1.151:1521/xe ",driver="oracle.jdbc.OracleDriver",dbtable="DEPT").load().take(10)

myrdd1 = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:sqlite:/home/sammy/snap/dbeaver-ce/94/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db")\
    .option("dbtable", "Album")\
    .option("driver", "org.sqlite.JDBC").load()

#my_writer = DataFrameWriter(myrdd1)

myrdd2 = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db")\
    .option("dbtable", "STG_FireIncidents")\
    .option("driver", "org.sqlite.JDBC").load()

#myrdd3 = my_writer.jdbc


if __name__ == '__main__':
    print('This is main')
#    print(my_writer)
    myrdd2.printSchema()
    myrdd1.write.csv(path="/home/sammy/Learning_pyspark/OutDir/album_out/fil.csv", mode="append",header="True")
    myrdd1.write.jdbc(url ="jdbc:sqlite:/home/sammy/Learning_pyspark/SQLITE/STG.db",table="STG_Album",mode="append")

    myrdd2.show(2)

    myrdd1.show(5)
    myrdd1.describe()
#    myrdd1.collect()
#    myrdd1.count()

    print("Connected SQLLite")
    print(myrdd1)

    print(myrdd1.count())

