from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

myRange = spark.range(1000).toDF("number")


# COMMAND ----------

divisBy2 = myRange.where("number % 2 = 0")

# COMMAND ----------

flightData2015 = spark \
    .read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .csv(
    "/home/vladyslav_podrazhanskyi/projects/python/Spark-The-Definitive-Guide/source_data/flight-source_data/csv/2015-summary.csv")

"""
>>> flightData2015.take(3)
[Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Romania', count=15), 
Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Croatia', count=1), 
Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Ireland', count=344)]
"""

# flightData2015.sort("count", ascending=False).show()


# COMMAND ----------


# You can make any DataFrame into a table or view with one simple method call:
flightData2015.createOrReplaceTempView("flight_data_2015")

# COMMAND ----------


sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, Sum(count) AS sum 
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum DESC
""")

# sql_df = spark.sql("""
# SELECT DEST_COUNTRY_NAME, Sum(count) AS sum
# FROM flightData2015
# GROUP BY DEST_COUNTRY_NAME
# ORDER BY sum DESC
# """)


dataFrameWay = flightData2015 \
    .groupBy("DEST_COUNTRY_NAME") \
    .sum("count") \
    .withColumnRenamed("sum(count)", "sum") \
    .orderBy("sum", ascending=False)

# sqlWay.explain()
# dataFrameWay.explain()


# COMMAND ----------

from pyspark.sql.functions import max

# flightData2015.select(max("count")).take(1)


# COMMAND ----------

maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

# maxSql.show()


# COMMAND ----------

flightData2015 \
    .groupBy("DEST_COUNTRY_NAME") \
    .sum("count") \
    .withColumnRenamed("sum(count)", "sum") \
    .sort(F.desc("sum")) \
    .limit(5) \
    .show()

spark.stop()

# flightData2015.show()


# flightData2015\
#   .groupBy("DEST_COUNTRY_NAME")\
#   .sum("count")\
#   .withColumnRenamed("sum(count)", "destination_total")\
#   .sort(desc("destination_total"))\
#   .limit(5)\
#   .show()
#
#
# # COMMAND ----------
#
# flightData2015\
#   .groupBy("DEST_COUNTRY_NAME")\
#   .sum("count")\
#   .withColumnRenamed("sum(count)", "destination_total")\
#   .sort(desc("destination_total"))\
#   .limit(5)\
#   .explain()


# COMMAND ----------

