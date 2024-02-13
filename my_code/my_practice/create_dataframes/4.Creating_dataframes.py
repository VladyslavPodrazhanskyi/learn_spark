"""
You can manually create a PySpark DataFrame
using toDF() and createDataFrame() methods,
both these function takes different signatures in order to create
DataFrame from existing RDD, list, and DataFrame.



"""

from my_code import ROOT

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

spark = SparkSession.builder.master("local[*]").getOrCreate()

# Create dataframes

# 1. from raw source_data sources - files  ( spark.read)
df = spark.read.format("json").load(f"{ROOT}/source_data/flight-data/json/2015-summary.json")

# temporary view for query with SQL
df.createOrReplaceTempView("dfTable")

spark.sql("select * from dfTable").show()
# 2. We can also create DataFrames on the fly by taking a set of rows and converting them to a DataFrame.
# ( schema, rows, spark.createDataFrame)

# myManualSchema = T.StructType([
#     T.StructField("some", T.StringType(), True),
#     T.StructField("col", T.StringType(), True),
#     T.StructField("names", T.IntegerType(), False)
# ])
#
#
# myRow1 = Row(some="Hello", col='sf', names=1)
#
# print(type(myRow1))  # <class 'pyspark.sql.types.Row'>
# print(myRow1[0])  #Hello
#
# myRow2 = Row("Bye", "Baby", 17)
# myRow3 = Row("Good morning", "Dear", 23)
# myRow4 = Row("Good evening", None, 5)
#
# # myDf = spark.createDataFrame([myRow1, myRow2, myRow3, myRow4], myManualSchema)
# myDf = spark.createDataFrame(data=[myRow1], schema=myManualSchema)
# # myDf.show()


myManualSchema = T.StructType([
    T.StructField("some", T.StringType(), True),
    T.StructField("col", T.StringType(), True),
    T.StructField("names", T.StringType(), True)
])

myRow1 = Row("Hello", 'sf', 'lsf')

print(type(myRow1))  # <class 'pyspark.sql.types.Row'>
print(myRow1[0])  # Hello

myRow2 = Row("Bye", "Baby", 17)
myRow3 = Row("Good morning", "Dear", 23)
myRow4 = Row("Good evening", None, 5)

myDfWithSchema = spark.createDataFrame([myRow1, myRow2, myRow3, myRow4], myManualSchema)
myDfWithSchema.show()

"""
+------------+----+-----+
|        some| col|names|
+------------+----+-----+
|       Hello|  sf|  lsf|
|         Bye|Baby|   17|
|Good morning|Dear|   23|
|Good evening|NULL|    5|
+------------+----+-----+

"""

myDfWithoutSchema = spark.createDataFrame(
    [myRow1]
)
myDfWithoutSchema.show()
"""
+-----+---+---+
|   _1| _2| _3|
+-----+---+---+
|Hello| sf|lsf|
+-----+---+---+
"""

# https://sparkbyexamples.com/pyspark/different-ways-to-create-dataframe-in-pyspark/?utm_content=cmp-true


columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

# 1. Create DataFrame from RDD

rdd = spark.sparkContext.parallelize(data)
print(rdd)          # ParallelCollectionRDD[22] at readRDDFromFile at PythonRDD.scala:289
print(type(rdd))    # <class 'pyspark.rdd.RDD'>

print(rdd.collect() == data)  # True

# 2. toDF() method is used to create a DataFrame from the existing RDD.
# Since RDD doesn’t have columns, the DataFrame is created with default column names “_1” and “_2” as we have two columns

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
"""
root
 |-- _1: string (nullable = true)
 |-- _2: string (nullable = true
"""

dfFromRDD2 = rdd.toDF(columns)
dfFromRDD2.printSchema()

