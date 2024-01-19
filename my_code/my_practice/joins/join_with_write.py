from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

from my_code import ROOT

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

days = ['10', '11', '12', '4', '5', '6', '7', '8', '9']

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


schema_api = st.StructType([
    st.StructField('api_log', st.StringType(), True),
    st.StructField('order_api_col', st.StringType(), True),
    st.StructField('day', st.StringType(), True)
])


schema_orders = st.StructType([
    st.StructField('orders', st.StringType(), True),
    st.StructField('order_dt_col', st.StringType(), True),
    st.StructField('order_api_col', st.StringType(), True),
    st.StructField('day', st.StringType(), True)
])

schema_dt = st.StructType([
    st.StructField('dt_id', st.StringType(), True),
    st.StructField('order_dt_col', st.StringType(), True),
    st.StructField('day', st.StringType(), True)
])


api_df = spark.createDataFrame(
    [
        ('api_log1', 'order_api_col1', '1'),
        ('api_log2', 'order_api_col2', '3'),
        ('api_log3', 'order_api_col3', '3'),
        ('api_log4', 'order_api_col4', '3'),
        ('api_log5', 'order_api_col5', '3'),
        ('api_log6', 'order_api_col6', '3'),
        ('api_log7', 'order_api_col7', '4'),
        ('api_log8', 'order_api_col8', '4'),
        ('api_log9', 'order_api_col9', '4')
    ],
    schema_api
).where(sf.col('day').isin(days))


orders_df = spark.createDataFrame(
    [
        ('order1', 'or_dt_1', 'order_api_col1', '3'),
        ('order2', 'or_dt_2', 'order_api_col2', '3',),
        ('order3', 'or_dt_3', 'order_api_col3', '3',),
        ('order4', 'or_dt_4', 'order_api_col4', '4',),
        ('order5', 'or_dt_5', 'order_api_col5', '4',),
        ('order6', 'or_dt_6', 'order_api_col6', '4',),
        ('order7', 'or_dt_7', 'order_api_col7', '5',),
        ('order8', 'or_dt_8', 'order_api_col8', '5',),
        ('order9', 'or_dt_9', 'order_api_col9', '5',)
    ],
    schema_orders
).where(sf.col('day').isin(days))


dt_df = spark.createDataFrame(
    [
        ('dt_1', 'or_dt_1', '5',),
        ('dt_2', 'or_dt_2', '5',),
        ('dt_3', 'or_dt_3', '5',),
        ('dt_4', 'or_dt_4', '6',),
        ('dt_5', 'or_dt_5', '6',),
        ('dt_6', 'or_dt_6', '6',),
        ('dt_7', 'or_dt_7', '7',),
        ('dt_8', 'or_dt_8', '7',),
        ('dt_9', 'or_dt_9', '7',)
    ],
    schema_dt

).where(sf.col('day').isin(days))


orders_api = orders_df.join(
    api_df,
    on='order_api_col',
    how='inner'
).drop(api_df.day)


joined_df = orders_api.join(
    dt_df,
    on='order_dt_col',
    how='inner'
).drop(dt_df.day)


joined_df.show()
joined_df.printSchema()


result_df = (
    spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{ROOT}/my_code/my_practice/joins/results/")
).where(sf.col('day').isin(days))


joined_df = joined_df.unionByName(result_df).dropDuplicates()

(
    joined_df.write
    .partitionBy("day")
    .mode("overwrite")
    .option('header', 'True')
    .csv(f"{ROOT}/my_code/my_practice/joins/results")
)








