from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

schema = st.StructType([
    st.StructField("id", st.StringType(), True),
    st.StructField('status_codes', st.ArrayType(
        st.IntegerType(),
        True), True)
])


data = [
    (
        '1',
        [200, 200]
    ),
    (
        '2',
        [100, 200]
    ),
    (
        '3',
        [200, 100]
    ),
    (
        '4',
        [400, 500]
    ),
(
        '5',
        [200, 200, 200]
    ),
    (
        '6',
        [200, 200, 200, 200]
    ),
    (
        '7',
        [400, 500, 200, 200]
    ),


]

df = spark.createDataFrame(
    data=data,
    schema=schema
)

df.show(truncate=False)

df_exists = df.filter(
    sf.exists(sf.col("status_codes"), lambda x: x != 200)
)
df_forall = df.filter(sf.forall(sf.col("status_codes"), lambda x: x != 200))

df_exists.show(truncate=False)


# is_even = lambda e: e % 2 == 0
# res = df.withColumn("all_even", sf.forall(sf.col("status_codes"), lambda x: x == 200))
# res.show()