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

from my_code import ROOT

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


"""
This creates a DataFrame with 3 partitions
 using a hash-based partition on state column.  
 The hash-based partition takes each state value, 
 hashes it into 3 partitions (partition = hash(state) % 3). 
 This guarantees that all rows with the same sate (partition key)
  end up in the same partition.

"""

repart_three_df = df.repartition(6, sf.col('state'))
print(repart_three_df.rdd.getNumPartitions())  # 6
# write to 4 files
(
    repart_three_df
    .write.mode("overwrite")
    .option('header', True)
    .csv(f"{ROOT}/code/my_practice/partitions/tmp/df_repart_three")
)

restored_df = (
    spark
    .read
    .option("header", True)
    .csv(f"{ROOT}/code/my_practice/partitions/tmp/df_repart_three/")
)
# receive 4 partitions
print(restored_df.rdd.getNumPartitions())

# save rdd 6 partitions but 2 are empty
repart_three_df.rdd.saveAsTextFile(f"{ROOT}/code/my_practice/partitions/tmp/rdd_repart_three/")