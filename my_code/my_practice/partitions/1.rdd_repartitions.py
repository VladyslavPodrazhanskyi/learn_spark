"""

https://sparkbyexamples.com/pyspark/pyspark-repartition-vs-coalesce/#google_vignette

"""

import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[5]").getOrCreate()


rdd = spark.sparkContext.parallelize(range(0, 20))
print("From local[5]: " + str(rdd.getNumPartitions()))  # From local[5]: 5

print(rdd.glom().collect())

#
#
# rdd1 = spark.sparkContext.parallelize(range(0, 25), 6)
# print("parallelize : " + str(rdd1.getNumPartitions()))   # parallelize : 6
#
# rddFromFile = spark.sparkContext.textFile(f"{ROOT}/source_data/test.txt", 10)
# print("TextFile : " + str(rddFromFile.getNumPartitions()))   # TextFile : 10
#
# rdd1.saveAsTextFile(f"{ROOT}/result_data/tmp/partition")
# rdd2 = rdd1.repartition(4)
# print("Repartition size : "+str(rdd2.getNumPartitions()))    # Repartition size : 4
# rdd2.saveAsTextFile(f"{ROOT}/result_data/tmp/re_partition")
#
# rdd3 = rdd1.coalesce(4)
# rdd2.saveAsTextFile(f"{ROOT}/result_data/tmp/coalesce")