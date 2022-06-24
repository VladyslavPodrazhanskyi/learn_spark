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
    ],
    schema=schema
)

cur_df.show()
df_with_is_eur = cur_df.withColumn(
    'is_Eur',
    sf.when(
        sf.col('currency') == 'EUR',
        sf.lit(True)
    ).otherwise(False)
)

df_with_is_eur.show()
df_with_is_eur.printSchema()


