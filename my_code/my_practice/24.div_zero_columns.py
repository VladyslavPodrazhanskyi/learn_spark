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

schema = st.StructType([
    st.StructField("id", st.IntegerType(), True),
    st.StructField("net", st.DecimalType(34, 5), True),
    st.StructField("gross", st.DecimalType(34, 5), True),
])


cur_df = spark.createDataFrame(
    [
        (1, Decimal('5'), Decimal('7')),
        (1, Decimal('5'), Decimal('5')),
        (1, Decimal('4'), Decimal('6')),
    ],
    schema=schema
)


calculated_df = (
    cur_df
    .withColumn(
        "calculations",
        sf.col('net') / (sf.col('gross') - sf.col('net'))
    )
)

calculated_df.show()

#
# | id|    net|  gross|calculations|
# +---+-------+-------+------------+
# |  1|5.00000|7.00000|    2.500000|
# |  1|5.00000|5.00000|        null|
# |  1|4.00000|6.00000|    2.000000|
# +---+-------+-------+------------+



