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
        ('value', 'EURO', 1.19),
        ('value', 'USD', None),
        ('value', 'UAH', 32.54),
        ('value', 'USD', 1.0),
        ('value', 'EURO', None),
        ('value', 'USD', None),
        ('value', 'EURO', 1.19),
        ('value', 'USD', None),
    ],
    schema=schema
)


df.show()

# filled_df = df.na.fill(value=1.0, subset=['direct_rate'])
# filled_df.show()

updated_df = df.withColumn(
    'direct_rate',
    sf.when((sf.col('currency') == 'USD'), 1.0)
    .otherwise(sf.col('direct_rate'))
)

updated_df.show()