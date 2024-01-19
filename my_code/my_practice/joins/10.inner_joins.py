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
    st.StructField('col1', st.IntegerType(), True),
    st.StructField('col2', st.DoubleType(), True),
    st.StructField('col3', st.StringType(), True),
])




left_df = spark.createDataFrame(
    [
        (1, 2.0, 'AD12'),
        (1, 2.28449, ''),
        (1, 4.000000000001, 'BP3')
    ],
    schema
)
right_df = spark.createDataFrame(
    [
        (1, 2.0, 'AD12'),
        (1, 2.28449, ''),
        (1, 4.000000000001, 'skf')
    ],
    schema
    # ("col1", "col2", "col3")
)

joined_df = left_df.join(
    right_df,
    on='col2',
    how='inner'
).drop(left_df.col1)


joined_df.show()
joined_df.printSchema()








