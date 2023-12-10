import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

catalog = spark.catalog
pprint(catalog.__sizeof__())
pprint(catalog.listDatabases())
pprint(catalog.listTables())

df = spark.read.format("json").load(f"{ROOT}/source_data/flight-data/json/")

df = df.withColumn('first', sf.lit(1))

df = df.withColumn('coalesce', sf.coalesce(sf.col('first'), sf.col('count')))

df.show()
print(df.count())

spark.stop()
