

import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st


spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.range(100)  # .withColumn("even", )
df.show()
