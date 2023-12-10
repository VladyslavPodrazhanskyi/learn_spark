"""
    mapPartitions

    It is similar to map() operation where the output of mapPartitions() returns the same number of rows as in input RDD.
    It is used to improve the performance of the map() when there is a need to do heavy initializations like Database connection.
    mapPartitions() applies a heavy initialization to each partition of RDD instead of each element of RDD.
    It is a Narrow transformation operation
    PySpark DataFrame doesn’t have this operation hence you need to convert DataFrame to RDD to use mapPartitions()

"""

import os
from pyspark import SparkContext, SparkConf
from my_code import ROOT

from pyspark.sql import SparkSess

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [
    ('James', 'Smith', 'M', 3000),
    ('Anna', 'Rose', 'F', 4100),
    ('Robert', 'Williams', 'M', 6200),
]

columns = ["firstname", "lastname", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()


# gen with yield
def reformat_gen(partition_data):
    for row in partition_data:
        print(type(row))
        firstname = row.firstname
        lastname = row.lastname
        name = firstname + ', ' + lastname
        gender = row.gender.lower()
        salary = row.salary
        yield name, gender, salary


# # gen with iter without yield
# def reformat_gen2(partition_data):
#     updated_data = []
#     for row in partition_data:
#         firstname = row.firstname
#         lastname = row.lastname
#         name = firstname + ', ' + lastname
#         gender = row.gender.lower()
#         salary = row.salary
#         updated_data.append((name, gender, salary))
#     return iter(updated_data)


map_part_rdd = df.rdd.mapPartitions(reformat_gen)
df2 = map_part_rdd.toDF(['name', 'gender', 'salary'])

df2.show()
