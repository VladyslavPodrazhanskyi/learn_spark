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


cur_df = spark.createDataFrame(
    [
        ('value', 'EUR', 1.19),
        ('value', 'UAH', 32.54),
        ('value', 'EUR', 1.0),
        ('value', 'UAH', 32.54),
        ('value', None, 32.54),
    ],
    schema=schema
)

cur_df.show()

print(cur_df.schema)
print(cur_df.schema.fields)
print(set(cur_df.schema.names))
# print(cur_df.schema.json)

