from my_code import ROOT

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.format("json").load(f"{ROOT}/source_data/flight-data/json/2015-summary.json")

# Schemas
# A schema defines the column names and types of a DataFrame. We can either let a source_data source
# define the schema (called schema-on-read) or we can define it explicitly ourselves.
# Deciding whether you need to define a schema prior to reading in your source_data depends on your use case.
# For ad hoc analysis, schema-on-read usually works just fine (although at times it can be a bit slow with
# plain-text file formats like CSV or JSON). However, this can also lead to precision issues like a long
# type incorrectly set as an integer when reading in a file. When using Spark for producti(on Extract,
# Transform, and Load (ETL), it is often a good idea to define your schemas manually, especially when
# working with untyped source_data sources like CSV and JSON because schema inference can vary depending
# on the type of source_data that you read in.


# Schema - StructType, made of fields
print(df.schema)    # StructType(List(StructField(DEST_COUNTRY_NAME,StringType,true),StructField(ORIGIN_COUNTRY_NAME,StringType,true),StructField(count,LongType,true)))
# StructType(List(
#      StructField(DEST_COUNTRY_NAME,StringType,true),
#      StructField(ORIGIN_COUNTRY_NAME,StringType,true),
#      StructField(count,LongType,true)
#      ))

print(type(df.schema))

df.printSchema()
"""
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: long (nullable = true)

"""

df.show()


# How to create and enforce a specific schema on a DataFrame


myManualSchema = T.StructType([
  T.StructField("DEST_COUNTRY_NAME", T.StringType(), True),
  T.StructField("ORIGIN_COUNTRY_NAME", T.StringType(), True),
  T.StructField("count", T.IntegerType(), False, metadata={"hello": "world"})
])


df2 = (
  spark
  .read.format("json")
  .schema(myManualSchema)
  .load(f"{ROOT}/source_data/flight-data/json/2015-summary.json")
)


print("df2")
print(df2.columns)
# ['DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME', 'number']

df2.printSchema()
# root
#  |-- DEST_COUNTRY_NAME: string (nullable = true)
#  |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
#  |-- number: string (nullable = true)


print(F.col("someColumnName"))                     #   Column<'someColumnName'>
print(type(F.col("someColumnName")))               #   <class 'pyspark.sql.column.Column'>

print(F.column("someColumnName"))                  #   Column<'someColumnName'>

print(df2.first())
df.show()


print(F.expr("(((someCol + 5) * 200) - 6) < otherCol"))  # Column<'((((someCol + 5) * 200) - 6) < otherCol)'>





