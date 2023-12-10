"""

eqNullSafe  - consideres null as definite value

"""
from pprint import pprint
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as T
import pyspark.sql.functions as F

# from utils import get_project_root
from my_code.utils import get_project_root


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


df = spark.createDataFrame([
    Row(id=1, value=25),
    Row(id=2, value=None),
    Row(id=2, value=-1)
])

df.show()

df1 = df.withColumn(
    'test_eq',
    F.col('value') == -1                # null
).withColumn(
    'test_eq_null',
    F.col('value').eqNullSafe(-1)       # false
)

df1.show()

df2 = df.filter(
    F.col('value') == -1
)

df2.show()

df3 = df.filter(
    F.col('value').eqNullSafe(-1)
)

df3.show()


df4 = df.filter(
    F.col('value') != -1     # 25 included, null not included
)

df4.show()


df5 = df.filter(
    ~F.col('value').eqNullSafe(-1)     # 25 and null included
)

df5.show()


compare_values_df = spark.createDataFrame([
    Row(id=1, value1=25, value2=25, ),
    Row(id=2, value1=None, value2=None),
    Row(id=2, value1=-1, value2=-4)
])


compare_values_df1 = compare_values_df.filter(
    F.col('value1') == F.col('value2')
)

compare_values_df1.show()

compare_values_df2 = compare_values_df.filter(
    F.col('value1').eqNullSafe(F.col('value2'))
)

compare_values_df2.show()

