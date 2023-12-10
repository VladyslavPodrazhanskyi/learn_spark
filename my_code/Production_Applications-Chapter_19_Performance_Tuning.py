# Original loading my_code that does *not* cache DataFrame
DF1 = spark.read.format("csv")\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .load("/source_data/flight-source_data/csv/2015-summary.csv")
DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
DF4 = DF1.groupBy("count").count().collect()


# COMMAND ----------

DF1.cache()
DF1.count()


# COMMAND ----------

DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
DF4 = DF1.groupBy("count").count().collect()


# COMMAND ----------

