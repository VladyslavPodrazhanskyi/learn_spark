from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.range(500).toDF("number")



# df1 = df.select(df["number"] + 10).withColumnRenamed('(number + 10)', "new_number")
df2 = df.select(F.col("number") + F.lit(10)).toDF("new_number")
df2.show()

# COMMAND ----------

spark.range(2).collect()

collected = df2.collect()
print(collected)                                  # [Row(new_number=10), Row(new_number=11), Row(new_number=12), Row(n
print([row.new_number for row in collected])     # [10, 11, 12, 13, 14,....


# COMMAND ----------


b = T.ByteType()
print(b)           # ByteType
print(type(b))     # <class 'pyspark.sql.types.ByteType'>
