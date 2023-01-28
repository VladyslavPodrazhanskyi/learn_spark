"""
https://sparkbyexamples.com/spark/spark-shuffle-partitions/

Spark SQL Shuffle Partitions

The Spark SQL shuffle is a mechanism for redistributing or re-partitioning data
so that the data is grouped differently across partitions,
based on your data size

you may need to reduce or increase the number
of partitions of RDD/DataFrame using:

- spark.sql.shuffle.partitions  configuration

- through code.

"""
from pyspark import RDD
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = spark.sparkContext

text_rdd = sc.textFile(f'{ROOT}/source_data/Gutenberg/pg19337.txt')

print(text_rdd.getNumPartitions())  # 2

mapped_rdd = text_rdd.flatMap(lambda x: x.split(' ')).map(lambda w: (w, 1))

print(mapped_rdd.getNumPartitions())  # 2

reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)

# Both getNumPartitions from the above examples
# return the same number of partitions.
# Though reduceByKey() triggers data shuffle,
# it doesn’t change the partition count
# as RDD’s inherit the partition size from parent RDD.

print(reduced_rdd.getNumPartitions())  # 2
print(reduced_rdd.collect())
