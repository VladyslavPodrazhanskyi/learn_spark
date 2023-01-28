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
    st.StructField("direct_rate", st.DoubleType(), True),
])

df = spark.createDataFrame(
    [
        ('value', 'EUR', 1.19),
        ('value', 'USD', 2.23),
        ('value', 'UAH', 32.54),
        ('value', 'USD', 1.0),
        ('value', 'UAH', 32.54),
        ('value', 'USD', None),
        ('value', 'EUR', 1.19),
        ('value', 'USD', None),
    ],
    schema=schema
)


df.show()

df.dropDuplicates(['currency']).show()
