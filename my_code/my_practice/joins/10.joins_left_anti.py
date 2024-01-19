from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

left_df = spark.createDataFrame([
    Row(id=1, value1=25, value2=25, ),
    Row(id=2, value1=None, value2=None),
    Row(id=3, value1=-1, value2=-4)
])

right_df = spark.createDataFrame([
    Row(id=1, value1=25, value2=25, ),
    Row(id=2, value1=None, value2=None),
    Row(id=3, value1=-1, value2=-4)
])

joined_df = left_df.join(
    right_df,
    left_df.id == right_df.id,
    'left'
)

joined_df.printSchema()
joined_df.show()


# (fni_api_logs_df.dri_user == dri_orders_df.user_id)
#     & (fni_api_logs_df.request_vin == dri_orders_df.vin)
#     & (fni_api_logs_df.response_sellrate == dri_orders_df.predicted_apr)