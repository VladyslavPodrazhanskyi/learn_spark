from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.range(500).toDF("number")
# df1 = df.select(df["number"] + 10).withColumnRenamed('(number + 10)', "new_number")
df2 = df.select(df["number"] + 10).toDF("new_number")
df2.show()

# COMMAND ----------

spark.range(2).collect()

collected = df2.collect()
print(collected)
print([row.new_number for row in collected])


# COMMAND ----------

from pyspark.sql.types import *
b = ByteType()


# COMMAND ----------

