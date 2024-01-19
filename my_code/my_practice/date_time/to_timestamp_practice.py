"""
https://sparkbyexamples.com/spark/pyspark-to_timestamp-convert-string-to-timestamp-type/#google_vignette


Syntax: to_timestamp(timestampString:Column)
default format: MM-dd-yyyy HH:mm:ss.SSS

Syntax: to_timestamp(timestampString:Column,format:String)


"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.createDataFrame(
    data=[
        ("1", "2019-06-24 12:01:19.000"),
        ("2", "2019-05-24 12:01:19.000"),
        ("3", "2023-06-24 12:01:19.000")
    ],
    schema=["id", "input_timestamp"])

df.printSchema()

# Timestamp String to DateType
df = df.withColumn("timestamp", sf.to_timestamp("input_timestamp"))
df.printSchema()
df.show(truncate=False)

# # Using Cast to convert TimestampType to DateType
df = df.withColumn(
    'timestamp_string', sf.to_timestamp('timestamp').cast('string')
).withColumn(
    'date', sf.to_date("timestamp")
).withColumn(
    'now', sf.current_timestamp()
).withColumn('current_date', sf.current_date())

df.printSchema()
df.show(truncate=False)

