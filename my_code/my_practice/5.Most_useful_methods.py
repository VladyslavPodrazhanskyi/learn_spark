import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()


# spark.sparkContext.setLogLevel("INFO")

# spark catalog
catalog = spark.catalog
pprint(catalog.__sizeof__())
pprint(catalog.listDatabases())
pprint(catalog.listTables())

# spark reader
df = spark.read.format("json").load(f"{ROOT}/source_data/flight-data/json/")


df = df.withColumnRenamed("ORIGIN_COUNTRY_NAME", "renamed_ORIGIN_COUNTRY_NAME")

df.show()
print(df.count())  # 1502
df.printSchema()
print(df.schema)


time.sleep(600)
spark.stop()





# df_with_five = df.withColumn("five", sf.lit(5.0))
# print(set(df_with_five.schema) - set(df.schema))
# print(set(df.schema) - set(df_with_five.schema))
#
# print(set(df.schema) -  set())


#
# print(df.select('count').rdd)  # MapPartitionsRDD[27] at javaToPython at NativeMethodAccessorImpl.java:0
# print(df.select('count').rdd.max())  # Row(count=370002)
# print(df.select('count').rdd.max()[0])
#
# print(df.select(sf.max('count').alias('max')).collect()[0]['max'])




#
# df.select("ORIGIN_COUNTRY_NAME", "count").show(5)
# df.selectExpr("ORIGIN_COUNTRY_NAME", "count").show(5)
#
# df.select(
#     "DEST_COUNTRY_NAME",
#     sf.expr("DEST_COUNTRY_NAME"),
#     sf.col("DEST_COUNTRY_NAME")
# ).show(5)

# advantage of F.expr in comparison with col
# df.select(F.expr("DEST_COUNTRY_NAME AS destination")).show(2)
# df.select(F.col("DEST_COUNTRY_NAME").alias("destination")).show(2)
#
# # selectExpr !!!
# """
# Because select followed by a series of expr is such a common pattern, Spark has a shorthand
# for doing this efficiently: selectExpr. This is probably the most convenient interface for
# everyday use
# This opens up the true power of Spark. We can treat selectExpr as a simple way to build up
# complex expressions that create new DataFrames.
# """
# df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(5)
#
# df.selectExpr(
#     "*",
#     "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry") \
#     .show(100)
#
#
# # aggregations over the entire DataFrame
#
# df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
#
#
#
# print("sqlWay")
#
# # temporary view for query with SQL
# df.createOrReplaceTempView("dfTable")
#
# sqlWayWithinCountry = spark.sql(
#     """
#     SELECT *,
#     (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
#     FROM dfTable
#     """
# ).filter(sf.col('withinCountry')).show(12)
#
# pprint(catalog.listTables()) # [Table(name='dftable', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]


# sqlWay = spark.sql("""
# SELECT
# DEST_COUNTRY_NAME, Sum(count) as sum
# FROM dfTable
# GROUP BY DEST_COUNTRY_NAME
# ORDER by sum DESC""").show(10)


