from datetime import date

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
    st.StructField("value", st.StringType(), True),
    st.StructField("select", st.StringType(), True),
    st.StructField("date", st.DateType(), True),
])

df = spark.createDataFrame(
    [
        ('value1', 'select1', date(2022, 4, 3)),
        ('value2', 'select2', date(2022, 4, 22)),
        ('value3', 'select3', date(2021, 7, 2)),
        ('value4', 'select4', date(2022, 8, 12)),
    ],
    ("value", "select", "date")
)


select_df = df.select([
    sf.col('select'),
    sf.year('date').alias('year'),
    sf.month('date').alias('month'),
    sf.concat(sf.col('select'), sf.lit(', '), sf.col('value')).alias('concat'),
    sf.lit(1).alias('one_col')
])

select_df.show()