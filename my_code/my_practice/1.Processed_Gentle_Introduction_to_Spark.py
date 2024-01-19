from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from my_code import ROOT


spark = SparkSession.builder.master("local[*]").getOrCreate()

myRange = spark.range(1000).toDF("number")


# COMMAND ----------

divisBy2 = myRange.where("number % 2 = 0")

# COMMAND ----------

flightData2015 = spark \
    .read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .csv(f"{ROOT}/source_data/flight-data/csv/2015-summary.csv")

flightData2015.show()

print(type(flightData2015.take(3)[0]))   # <class 'pyspark.sql.types.Row'>

"""
>>> flightData2015.take(3)
# [Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Romania', count=15),
# Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Croatia', count=1),
# Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Ireland', count=344)]
# """

flightData2015.sort("count", ascending=False).show()

(
    flightData2015
    .where(F.col("DEST_COUNTRY_NAME") != 'United States')
    .sort("count", ascending=False)
    .show()
)

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

sqlWay.show()

dataFrameWay = flightData2015 \
    .groupBy("DEST_COUNTRY_NAME") \
    .sum("count") \
    .withColumnRenamed("sum(count)", "sum") \
    .orderBy("sum", ascending=False)
#

(
    flightData2015
    .where(F.col('DEST_COUNTRY_NAME') != "Unites States")
    .explain()
)

# == Physical Plan ==

# *(1) Filter (isnotnull(DEST_COUNTRY_NAME#20) AND NOT (DEST_COUNTRY_NAME#20 = Unites States))

# FileScan csv [DEST_COUNTRY_NAME#20,ORIGIN_COUNTRY_NAME#21,count#22]
# Batched: false,
# DataFilters: [],
# Format: CSV,
# Location: InMemoryFileIndex(1 paths)[file:/home/vladyslav_podrazhanskyi/projects/PERSONAL/python/learn_spar...,
# PartitionFilters: [],
# PushedFilters: [],
# ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:int>


(
    flightData2015
    .sort('count', ascending=False)
    .explain()
)

# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- Sort [count#22 ASC NULLS FIRST], true, 0
#    +- Exchange rangepartitioning(count#22 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [id=#124]
#       +- FileScan csv[DEST_COUNTRY_NAME  # 20,ORIGIN_COUNTRY_NAME#21,count#22]....


sqlWay.explain()

# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- Sort [sum#77L DESC NULLS LAST], true, 0
#    +- Exchange rangepartitioning(sum#77L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [id=#144]
#       +- HashAggregate(keys=[DEST_COUNTRY_NAME#20], functions=[sum(count#22)])
#          +- Exchange hashpartitioning(DEST_COUNTRY_NAME#20, 200), ENSURE_REQUIREMENTS, [id=#141]
#             +- HashAggregate(keys=[DEST_COUNTRY_NAME#20], functions=[partial_sum(count#22)])
#                +- FileScan csv [DEST_COUNTRY_NAME#20,count#22] Batched: false, DataFil

dataFrameWay.explain()
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- Sort [sum#103L DESC NULLS LAST], true, 0
#    +- Exchange rangepartitioning(sum#103L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [id=#164]
#       +- HashAggregate(keys=[DEST_COUNTRY_NAME#20], functions=[sum(count#22)])
#          +- Exchange hashpartitioning(DEST_COUNTRY_NAME#20, 200), ENSURE_REQUIREMENTS, [id=#161]
#             +- HashAggregate(keys=[DEST_COUNTRY_NAME#20], functions=[partial_sum(count#22)])
#                +- FileScan csv

# COMMAND ----------

print(flightData2015.select(F.max("count")).take(1))  # [Row(max(count)=370002)]

# COMMAND ----------

maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

print('maxSql')
maxSql.show()
#
#
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


