from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


df = spark.createDataFrame(
    [(1, 2, 3), (1, 2, 4), (1, 4, 6)],
    ("col1", "col2", "col3")
)

df.show()

df.dropDuplicates(["col1"]).show()
df.dropDuplicates(["col1", "col2"]).show()

df.createOrReplaceTempView("table")

spark.sql(
    """ 
    (
        SELECT col1, col2 FROM table
        group by col1, col2
    )
    """
).show()
