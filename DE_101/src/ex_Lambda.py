from pyspark.sql import SparkSession
from pyspark import SparkContext


from DE_101.src.Common import String_UDF

sc = SparkContext()

spark = SparkSession.builder.master("local[2]").getOrCreate()

##Extract
fireIncidentsDF = spark.read.load("/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv",format="csv", sep=",", inferSchema="true", header="true")
fireCallDF = spark.read.load("/home/sammy/Learning_pyspark/InDir/Fire_Department_Calls_for_Service.csv",format="csv", sep=",", inferSchema="true", header="true")

#fireCallDF.printSchema()
#fireIncidentsDF.printSchema()

mx=lambda a,b,c: a if a>b and a>c else b if b>a and b>c else c

print(mx(1,9,58))

x='new delhi'

y =String_UDF.to_camelcase(x)
print y

cmcase = lambda x:String_UDF.to_camelcase(x)

print(cmcase('san francisco'))


# Load a text file and convert each line to a Row.
lines = sc.textFile('/home/sammy/Learning_pyspark/InDir/Fire_Incidents.csv')
parts = lines.map(lambda l: l.split(","))
print parts.take(3)
lst = parts.take(2)

print(list(map(cmcase,y for y in lst)))


'''
for x in lst:
    print x
    for y in x:
        print y
'''
print parts.count()
