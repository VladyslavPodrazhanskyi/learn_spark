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
    Row(id=3, value1=-1, value2=-4)
])

compare_values_df.filter(F.col('value1') == F.col('value2')).show()

compare_values_df.filter(F.col('value1').eqNullSafe(F.col('value2'))).show()


# eqNullSafe join:

right_df = spark.createDataFrame([
    Row(idr=1, value1=25, value=None),
    Row(idr=2, value1=None, value=45),
    Row(idr=3, value1=-5, value=-6)
])

join_cond_eq = [compare_values_df.value1 == right_df.value1]
join_cond_eqNullSafe = [compare_values_df.value1.eqNullSafe(right_df.value1)]

compare_values_df.join(right_df, on=join_cond_eq, how='inner').show()
''''
+---+------+------+---+------+-----+
| id|value1|value2|idr|value1|value|
+---+------+------+---+------+-----+
|  1|    25|    25|  1|    25| NULL|
+---+------+------+---+------+-----+
'''

compare_values_df.join(right_df, on=join_cond_eqNullSafe, how='inner').show()

"""
+---+------+------+---+------+-----+
| id|value1|value2|idr|value1|value|
+---+------+------+---+------+-----+
|  2|  NULL|  NULL|  2|  NULL|   45|
|  1|    25|    25|  1|    25| NULL|
+---+------+------+---+------+----
"""

