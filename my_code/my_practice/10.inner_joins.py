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
        (1, 2, 'AD12'),
        (1, 2, ''),
        (1, 4, 'BP3')
    ],
    ("col1", "col2", "col3")
)
right_df = spark.createDataFrame(
    [
        (1, 2, 'AD12'),
        (1, 2, ''),
        (1, 4, 'skf')
    ],
    ("col1", "col2", "col3")
)

joined_df = left_df.join(
    right_df,
    on='col3',
    how='inner'
).drop(left_df.col1)


joined_df.show()
joined_df.printSchema()








