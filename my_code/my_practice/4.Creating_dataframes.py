from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


spark = SparkSession.builder.master("local[*]").getOrCreate()

# Create dataframes

# 1. from raw source_data sources - files  ( spark.read)
df = spark.read.format("json").load("source_data/flight-source_data/json/2015-summary.json")

# temporary view for query with SQL
# temporary view for query with SQL
df.createOrReplaceTempView("dfTable")

# 2. We can also create DataFrames on the fly by taking a set of rows and converting them to a DataFrame.
# ( schema, rows, spark.createDataFrame)
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
myManualSchema = StructType([
    StructField("some", StringType(), True),
    StructField("col", StringType(), True),
    StructField("names", IntegerType(), False)
])

from pyspark.sql import Row
myRow1 = Row("Hello", None, 1)

print(type(myRow1))  # <class 'pyspark.sql.types.Row'>
print(myRow1[0])  #Hello

myRow2 = Row("Bye", "Baby", 17)
myRow3 = Row("Good morning", "Dear", 23)
myRow4 = Row("Good evening", None, 5)

myDf = spark.createDataFrame([myRow1, myRow2, myRow3, myRow4], myManualSchema)
myDf.show()

