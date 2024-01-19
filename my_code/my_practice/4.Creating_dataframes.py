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
# temporary view for query with SQL
df.createOrReplaceTempView("dfTable")

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
print(myRow1[0])  #Hello

# myRow2 = Row("Bye", "Baby", 17)
# myRow3 = Row("Good morning", "Dear", 23)
# myRow4 = Row("Good evening", None, 5)

# myDf = spark.createDataFrame([myRow1, myRow2, myRow3, myRow4], myManualSchema)
myDf = spark.createDataFrame(
    [myRow1]
)

# myDf.show(



