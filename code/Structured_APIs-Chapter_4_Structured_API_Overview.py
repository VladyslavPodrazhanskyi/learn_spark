from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.range(500).toDF("number")
df.select(df["number"] + 10)


# COMMAND ----------

spark.range(2).collect()


# COMMAND ----------

from pyspark.sql.types import *
b = ByteType()


# COMMAND ----------

