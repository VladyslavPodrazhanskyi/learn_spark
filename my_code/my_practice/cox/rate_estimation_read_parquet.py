from pprint import pprint
from pyspark.sql import SparkSession
import pyspark.sql.types as st
import pyspark.sql.functions as sf

from my_code import ROOT

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)
spark.conf.set('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED')

rate_estimation_df = (
    spark
    .read
    .parquet(f"{ROOT}/source_data/cox_data/fni_rate_estimation")
)

# rate_estimation_df.show()
# rate_estimation_df.count()
# rate_estimation_df.printSchema()

rate_estimation_sample = rate_estimation_df.sample(fraction=0.0005)

rate_estimation_sample.write.parquet(f"{ROOT}/source_data/cox_data/fni_rate_estimation_sample/")

# ltv_hist_df = (
#     spark
#     .read
#     .parquet(f"{ROOT}/source_data/cox_data/ltv/ltv/")
# ).withColumn(
#     'year',
#     sf.year(sf.col("SBMT_date"))
# ).withColumn(
#     'month',
#     sf.month(sf.col("SBMT_date"))
# ).withColumn(
#     'day',
#     sf.dayofmonth(sf.col("SBMT_date"))
# ).select(
#     "VIN_NUM",
#     "ADJUSTED_VALUE",
#     "SBMT_date",
#     "MILEAGE"
# ).orderBy(
#     "MILEAGE"
# ).dropDuplicates().cache()
#
# ltv_hist_df.show(1000, truncate=False)
# ltv_hist_df.printSchema()
#
# print(ltv_hist_df.count())
#
#
# date_interval =  ltv_hist_df.select(
#     sf.max(sf.col("SBMT_date")),
#     sf.min(sf.col("SBMT_date")),
# ).collect()
#
# print(date_interval)