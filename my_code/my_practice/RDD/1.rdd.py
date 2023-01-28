import os
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("rdd_one").setMaster("local[*]")  # master is a Spark, Mesos or YARN cluster URL
sc = SparkContext(conf=conf)

# 2 ways of create RDD:

#  1) parallelizing an existing collection

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
print(distData.take(2))
print(distData.reduce(lambda a, b: a + b))



#  2) from file

print(os.getcwd())

lines = sc.textFile("source_data.txt")
print(type(lines))
print(lines.collect())  # better use take(n) to sample source_data not to overload the driver
lineLengths = lines.map(lambda s: len(s))
# lineLengths.persist()

totalLength = lineLengths.reduce(lambda a, b: a + b)
print(totalLength)
print(type(lineLengths))


# 3) using UDF for rdd

def number_words(s):
    words = s.split(" ")
    return len(words)


num_words_rdd = sc.textFile("source_data.txt").map(number_words)
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

lines = sc.textFile("source_data.txt")
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
# sorted_counts = counts.sortByKey()
print(counts.collect())