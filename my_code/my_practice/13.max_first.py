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
    [
        (1, 'xyz', 100),
        (2, 'abc', 200),
        (3, 'xyz', 300),
        (3, 'xyz', 400),
    ],
    ("col1", "col2", "col3")
)

df.show()

# order inside groupBy influence on order of unindicated columns




max_col1 = df.select('col1').orderBy(sf.col('col1').desc()).first()
max_col1_alt = df.select(sf.max(sf.col('col1'))).alias('max_col').collect()[0][0]


print(max_col1_alt)