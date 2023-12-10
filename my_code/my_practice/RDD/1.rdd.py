import os
from pyspark import SparkContext, SparkConf
from my_code import ROOT

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('rdd').getOrCreate()
# conf = SparkConf().setAppName("rdd_one").setMaster("local[*]")  # master is a Spark, Mesos or YARN cluster URL
sc = spark.sparkContext

# 2 ways of create RDD:

#  1) parallelizing an existing collection

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print(type(distData))                           # <class 'pyspark.rdd.RDD'>
print(distData.take(2))                         # [1, 2]
print(distData.reduce(lambda a, b: a + b))      # 15

#  2) from file

print(os.getcwd())

lines = sc.textFile(f"{ROOT}/my_code/my_practice/RDD/data.txt")
print(type(lines))
print(lines.collect())  # better use take(n) to sample source_data not to overload the driver
lineLengths = lines.map(lambda s: len(s))

lineLengths.persist()

print(type(lineLengths))            #  <class 'pyspark.rdd.PipelinedRDD'>
print(lineLengths.collect())

totalLength = lineLengths.reduce(lambda a, b: a + b)
print(totalLength)

# 3) using UDF for rdd
print("using UDF for rdd")


def number_words(s):
    words = s.split(" ")
    return len(words)


num_words_rdd = sc.textFile(f"{ROOT}/my_code/my_practice/RDD/data.txt").map(number_words)
totalNumWords = num_words_rdd.reduce(lambda a, b: a + b)
print(totalNumWords)


# 4) Key-Value:
'''
While most Spark operations work on RDDs containing any type of objects, 
a few special operations are only available on RDDs of key-value pairs. 
The most common ones are distributed “shuffle” operations, 
such as grouping or aggregating the elements by a key.

In Python, these operations work on RDDs containing built-in Python tuples such as (1, 2).
Simply create such tuples and then call your desired operation.
'''

lines = sc.textFile(f"{ROOT}/my_code/my_practice/RDD/data.txt")
print('type lines', type(lines))                      # type lines <class 'pyspark.rdd.RDD'>
pairs = lines.map(lambda s: (s, 1))
print('type paires', type(pairs))                     # type paires <class 'pyspark.rdd.PipelinedRDD'>
counts = pairs.reduceByKey(lambda a, b: a + b).cache()
print('type counts', type(counts))                    # type counts <class 'pyspark.rdd.PipelinedRDD'>
# sorted_counts = counts.sortByKey()
print(counts.collect())

#######################################################################

dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
debt_rdd = spark.sparkContext.parallelize(dept)
print("debt_rdd type", type(debt_rdd))  # debt_rdd type <class 'pyspark.rdd.RDD'>
debt_rdd = debt_rdd.map(lambda x: (x[0], x[1] + 5))
print("debt_rdd type", type(debt_rdd))    # debt_rdd type <class 'pyspark.rdd.PipelinedRDD'>
debt_df = debt_rdd.toDF()
debt_df.show()



