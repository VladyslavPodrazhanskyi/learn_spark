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
import random

from pyspark import RDD
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions', '50')

sc = spark.sparkContext

names = [
    'Martin',
    'Alex',
    'Peter',
    'Lena',
    'Olya',
    'Tanya',
    'Zhanna',
    'Ira',
    'Anya',
    'Nadya',
    'Andrew',
    'Gallya',
    'Semen',
]

large_data = [(random.choice(names), random.randrange(1000, 15000)) for i in range(1000000)]
large_schema = ('name', 'salary')


df = spark.createDataFrame(large_data, large_schema).cache()
age_df = spark.createDataFrame([(name, random.randrange(5, 70)) for name in names], ['name', 'age'])
age_df.show()
df.show()
print(df.count())
print(df.rdd.getNumPartitions())  # 8
print(age_df.rdd.getNumPartitions())  # 8


# types of joins

# broadcast  (Broadcast Hash Join) - does not change shuffle_partitions as there is no any shuffles
# MERGE, SHUFFLE_HASH -  200 by default or according spark.sql.shuffle.partitions
#  SHUFFLE_REPLICATE_NL  -  64 with dif keys in every partitions

joined = df.join(
    age_df.hint('SHUFFLE_REPLICATE_NL'),  # broadcast, MERGE, SHUFFLE_HASH
    on='name',
    how='inner'
).cache()

joined.printSchema()
joined.show()
print(joined.count())
print(joined.rdd.getNumPartitions())  # 50 or 200 by default


(
    joined
    .write
    .format("json")
    .mode("overwrite")
    .save(f"{ROOT}/my_code/my_practice/partitions/tmp/json")
)