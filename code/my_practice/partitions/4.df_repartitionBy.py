"""
https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-partitionby/

PySpark repartition() is a DataFrame method that is used
to increase or reduce the partitions in memory and when written to disk,
it create all part files in a single directory.

PySpark partitionBy() is a method of DataFrameWriter class
which is used to write the DataFrame to disk in partitions,
one sub-directory for each unique value in partition columns.
"""

import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from code import ROOT

spark = (
    SparkSession
    .builder
    .appName('Repartition_dataframe')
    .master("local[5]")
    .getOrCreate()
)

df = (
    spark.read.option("header", True)
    .csv(f"{ROOT}/code/my_practice/partitions/simple_zip_codes.csv")
)
df.printSchema()
df.show()
print(df.rdd.getNumPartitions())  # 1

# divide 12 partitions -  1 country, 6 states, 2 partitions per state
(
    df
    .repartition(2)
    .write
    .option('header', True)
    .partitionBy('Country', 'State')
    .mode('overwrite')
    .csv(f"{ROOT}/code/my_practice/partitions/tmp/df_partitionBy/")
)

# read partioned data
restored_partitioned_df = (
    spark
    .read
    .option('header', True)
    .csv(f"{ROOT}/code/my_practice/partitions/tmp/df_partitionBy/")
)

restored_partitioned_df.show()
print(restored_partitioned_df.rdd.getNumPartitions()) # 4
