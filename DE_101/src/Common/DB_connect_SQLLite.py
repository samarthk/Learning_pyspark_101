

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

#"jdbc:oracle:thin:username/password@//hostname:portnumber/SID"

#myrdd1 = spark.read.format("jdbc").options(url="jdbc:oracle:thin:user2/user2@//192.168.1.151:1521/xe ",driver="oracle.jdbc.OracleDriver",dbtable="DEPT").load().take(10)

myrdd1 = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:sqlite:/home/sammy/snap/dbeaver-ce/90/.local/share/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db")\
    .option("dbtable", "Album")\
    .option("driver", "org.sqlite.JDBC").load()




if __name__ == '__main__':
    print('This is main')
    myrdd1.printSchema()
    myrdd1.show()
    myrdd1.describe()
    myrdd1.collect()
    myrdd1.count()

    print("Connected SQLLite")
    print(myrdd1)

    print(myrdd1.count())

