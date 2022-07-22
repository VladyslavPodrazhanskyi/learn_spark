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

# spark.sparkContext.setLogLevel("INFO")

schema = st.StructType([
    st.StructField("id", st.IntegerType(), True),
    st.StructField("net", st.DecimalType(34, 5), True),
    st.StructField("gross", st.DecimalType(34, 5), True),
])


cur_df = spark.createDataFrame(
    [
        (1, Decimal('5'), Decimal('7')),
        (1, Decimal('5'), Decimal('5')),
        (1, None, Decimal('6')),
        (None, None, None),
    ],
    schema=schema
)

cur_df.show()

# count1 - method and count2 - function are different functions

print(cur_df.count())   # count1 -  nulls are counted


cur_df.select(
    sf.count('net').alias('count_net'),  # count2  - nulls are not counted
    sf.count('gross').alias('count_gross')  # count2
).show()