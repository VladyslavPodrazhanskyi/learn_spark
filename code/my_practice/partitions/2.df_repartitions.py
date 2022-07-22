"""
https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-coalesce/#google_vignette

you can’t specify the partition/parallelism
while creating DataFrame.
DataFrame by default internally uses the methods
specified in Section 1 to determine the default partition
and splits the data for parallelism.

PySpark repartition() is a DataFrame method that is used
to increase or reduce the partitions in memory and when written to disk,
it create all part files in a single directory.

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
spark.conf.set('spark.sql.shuffle.partitions', '50')  # default is 200

df = spark.range(0, 20)
print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv(f"{ROOT}/result_data/tmp/df_range")

df_repart_ten = df.repartition(10)
print(df_repart_ten.rdd.getNumPartitions())
df_repart_ten.write.mode("overwrite").csv(f"{ROOT}/result_data/tmp/df_repartition_ten")

df_repart_three = df.repartition(3)
print(df_repart_three.rdd.getNumPartitions())
df_repart_three.write.mode("overwrite").csv(f"{ROOT}/result_data/tmp/df_repartition_three")

df_coales_ten = df.coalesce(10)  # NumPartitions can't be increased
print(df_coales_ten.rdd.getNumPartitions())  # 5
df_coales_ten.write.mode("overwrite").csv(f"{ROOT}/result_data/tmp/df_coales_ten")

df_coales_three = df.coalesce(3)
print(df_coales_three.rdd.getNumPartitions())
df_coales_three.write.mode("overwrite").csv(f"{ROOT}/result_data/tmp/df_coales_three")

df.show()

df_group = df.groupBy('id').count()
print(df_group.rdd.getNumPartitions())

