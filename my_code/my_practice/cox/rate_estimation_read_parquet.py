"""
csv file - test source_data
schema

read csv with schema

write json

read json as df with schema ( otherwise time to string)

"""


from pprint import pprint
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from my_code import ROOT

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


api_csv_df = (
    spark
    .read
    .option("header", True)
    .options(inferSchema='True')
    .options(delimiter='|')
    .csv(f"{ROOT}/source_data/cox_data/api/dri_fni_09012022.csv")
).cache()

selected_df = api_csv_df.select(
    F.col('~CREDIT_APP_ID'),
    F.col('~BUY_RATE'),
).cache()

filtered_df = selected_df.filter(
    F.col('~CREDIT_APP_ID') == '~310300000091498291'
)

selected_df.show()
filtered_df.show()


# no data: 22, 23, 25, 26, 27, 29, 30
# 24.08.2022:  dri_fni_08242022.csv  BUY_RATE=~4.350000
# 28.08.2022:  dri_fni_08242022.csv  BUY_RATE=~
# 31.08.2022:  dri_fni_08242022.csv  BUY_RATE=~