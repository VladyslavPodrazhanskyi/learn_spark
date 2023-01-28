"""
==          ( null == value) -  result null
eqNullSafe  ( null eqNullSafe value)  -  result False

==          ( null == null) -  result null
eqNullSafe  ( null eqNullSafe null)  -  result True

"""


from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

schema = st.StructType([
    st.StructField("string", st.StringType(), True),
    st.StructField("bool", st.BooleanType(), True),
    st.StructField("int", st.IntegerType(), True),
])

df = spark.createDataFrame(
    [
        ('string1', True, 25),
        ('string2', False, None),
        (None, None, 14),
        ('string3', True, -7),
    ],
    schema=schema
)


df.filter(
    sf.col('bool').eqNullSafe(None)
).show()

df.filter(
    sf.col('bool') == None
).show()

