from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


left_df = spark.createDataFrame([(1, 2, 3), (1, 2, 4), (1, 4, 6)], ("col1", "col2", "col3"))
right_df = spark.createDataFrame([(1, 2, 3), (1, 2, 4), (1, 4, 6)], ("col1", "col2", "col3"))

joined_df = left_df.join(
    right_df,
    on='col3',
    how='left'
).drop(left_df.col1)



joined_df.show()
joined_df.printSchema()








