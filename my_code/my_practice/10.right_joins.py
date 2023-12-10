from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


left_df = spark.createDataFrame(
    [
        (1, 2, 'AD12', 4, 11, 'str1'),
        (1, 2, None, 5, -1, 'str2'),
        (1, 4, 'BP3', 6, -12, 'str3')
    ],
    ("com_col1", "com_col2", "com_col3", "lcol1", "lcol2", "lcol3")
)
right_df = spark.createDataFrame(
    [
        (1, 2, 'AD12', True, 11),
        (1, 2, 'n/a', True, -23),
        (1, 4, 'BP3', False, 2)
    ],
    ("com_col1", "com_col2", "com_col3", "rcol1", "rcol2")
)

left_df = left_df.fillna('n/a', subset='com_col3')


left_joined_df = left_df.join(
    right_df,
    on=["l1com_co", "com_col2", "com_col3"],
    how='inner'
)

left_joined_df.show()
left_joined_df.printSchema()








