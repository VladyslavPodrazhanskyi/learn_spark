from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.format("json").load("source_data/flight-source_data/json/2015-summary.json")

# Schemas
# A schema defines the column names and types of a DataFrame. We can either let a source_data source
# define the schema (called schema-on-read) or we can define it explicitly ourselves.
# Deciding whether you need to define a schema prior to reading in your source_data depends on your use case.
# For ad hoc analysis, schema-on-read usually works just fine (although at times it can be a bit slow with
# plain-text file formats like CSV or JSON). However, this can also lead to precision issues like a long
# type incorrectly set as an integer when reading in a file. When using Spark for production Extract,
# Transform, and Load (ETL), it is often a good idea to define your schemas manually, especially when
# working with untyped source_data sources like CSV and JSON because schema inference can vary depending
# on the type of source_data that you read in.

print(df.schema)
# Schema - StructType, made of fields

# StructType(List(
#      StructField(DEST_COUNTRY_NAME,StringType,true),
#      StructField(ORIGIN_COUNTRY_NAME,StringType,true),
#      StructField(count,LongType,true)
#      ))
df.show()


# How to create and enforce a specific schema on a DataFrame

from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])

# df = spark.read.format("json").schema(myManualSchema)\
#   .load("/source_data/flight-source_data/json/2015-summary.json")

df2 = spark.read.format("json").schema(myManualSchema).load("source_data/flight-source_data/json/2015-summary.json")
print("df2")
print(df2.columns)
# ['DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME', 'number']
df2.printSchema()
# root
#  |-- DEST_COUNTRY_NAME: string (nullable = true)
#  |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
#  |-- number: string (nullable = true)

# code is not working as it is possible to ref to column only in the context of df
# from pyspark.sql.functions import col, column
# col("someColumnName")
# column("someColumnName")

print(df2.first())
df.show()


# from pyspark.sql.functions import expr
# expr("(((someCol + 5) * 200) - 6) < otherCol")





