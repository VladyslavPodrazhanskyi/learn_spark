"""
eqNullSafe  - consideres null as definite value
"""

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as T
import pyspark.sql.functions as F



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

print('df1')
df1 = df.withColumn(
    'test_eq',
    F.col('value') == -1              # null
).withColumn(
    'test_eq_self',
    F.col('value').eqNullSafe(-1)       # false
).withColumn(
    'is_null',
    F.col('value').isNull()
)

df1.show()

df.filter(F.col('value') == -1).show()

df.filter(F.col('value').eqNullSafe(-1)).show()

df.filter(F.col('value') != -1).show()               # 25 included, null not included

df.filter(~F.col('value').eqNullSafe(-1)).show()     # 25 and null included

compare_values_df = spark.createDataFrame([
    Row(id=1, value1=25, value2=25, ),
    Row(id=2, value1=None, value2=None),
    Row(id=2, value1=-1, value2=-4)
])

compare_values_df.filter(F.col('value1') == F.col('value2')).show()

compare_values_df.filter(F.col('value1').eqNullSafe(F.col('value2'))).show()
