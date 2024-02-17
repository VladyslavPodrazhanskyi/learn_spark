import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

# spark reader
df = (
    spark
    .read.format("json")
    .load(f"{ROOT}/source_data/flight-data/json/")
)

df = df.withColumn('double', sf.col("count") * 2)

df.show()
print(df.count())  # 1502

# after groupBy used python function sum and avg without agg
df.groupby('DEST_COUNTRY_NAME').sum("count", "double").show()
df.groupby('DEST_COUNTRY_NAME').avg("count", "double").show()

# after groupBy used pyspark.sql.functions sum and avg inside agg



(
    df
    .groupby('DEST_COUNTRY_NAME')
    .agg(
        sf.sum("count").alias('sum_count'),
        sf.avg("count").alias('avg_count'),
        sf.sum("double").alias('sum_double'),
        sf.avg("double").alias('avg_double')
    ).show()
)


