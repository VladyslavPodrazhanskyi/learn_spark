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
    st.StructField("stable_column", st.StringType(), True),
    st.StructField("currency", st.StringType(), True),
    st.StructField("cur_value", st.DoubleType(), True),
])

df = spark.createDataFrame(
    [
        ('value_eur', 'EUR', 100.0),
        ('value_uah', 'UAH', 100.0),
        ('value_usd', 'USD', 100.0),
    ],
    schema=schema
)


cur_schema = st.StructType([
    st.StructField("stable_column", st.StringType(), True),
    st.StructField("currency", st.StringType(), True),
    st.StructField("direct_rate", st.DoubleType(), True),
])

cur_df = spark.createDataFrame(
    [
        ('value', 'EUR', 1.19),
        ('value', 'UAH', 32.54),
        ('value', 'USD', 1.0),
        ('value', 'UAH', 32.54),
    ],
    schema=schema
)


converted = df.join(
    cur_df,
    on=['currency'],
    how='left'
)

converted.show()


