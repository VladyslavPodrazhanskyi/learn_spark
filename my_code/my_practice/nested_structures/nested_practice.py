from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

spark = SparkSession.builder.master("local[*]").getOrCreate()

