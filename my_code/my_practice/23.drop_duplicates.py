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
    st.StructField("value", st.StringType(), True),

])


cur_df = spark.createDataFrame(
    [
        (1, 'value1.1'),
        (2, 'avalue2.1.'),
        (2, 'zzzzz'),
        (2, 'vvalue2.1.'),
        (2, 'qvalue2.1.'),
        (3, 'value3.1'),
    ],
    schema=schema
)


for i in range(10):
    cur_df.dropDuplicates(
        ['id']
    ).show()




