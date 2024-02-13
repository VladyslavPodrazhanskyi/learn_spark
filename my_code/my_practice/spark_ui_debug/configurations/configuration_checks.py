import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

conf = spark.sparkContext.getConf()

for k, v in conf.getAll():
    print(f'{k}: {v}')


app_name = conf.get("spark.app.name")
num_executors = conf.get("spark.executor.instances")
memory_per_executor = conf.get("spark.executor.memory")


print('\n specific configurations: \n')


print(f"Application Name: {app_name}")
print(f"Number of Executors: {num_executors}")
print(f"Memory per Executor: {memory_per_executor}")

# Stop the Spark session
spark.stop()