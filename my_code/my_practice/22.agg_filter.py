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
        ('value', 'EUR', -1.19),
        ('value', 'UAH', 32.54),
    ],
    schema=schema
)

cur_df.show()

agg = (
    cur_df
    .groupBy("currency")
    .agg(
        sf.when(
            (sf.sum('cur_value') != 0),
            sf.sum('cur_value')
        ).alias('sum_cur')
    )
)

agg.show()
#
# +--------+-------+
# |currency|sum_cur|
# +--------+-------+
# |     EUR|   null|
# |     UAH|  65.08|
# +--------+-------+

new_agg = (
    cur_df
    .groupBy("currency")
    .agg(sf.sum('cur_value').alias('sum_cur'))
    .filter(sf.col('sum_cur') != 0)
    .withColumn(
        'oppose_sum',
        1 / sf.col('sum_cur')
    ).drop('sum_cur')
)

new_agg.show()



