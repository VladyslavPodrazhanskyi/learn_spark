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

df.groupBy(
    sf.col('col1'),
    sf.col('col2'),
).agg(sf.sum(sf.col('col3'))).show()

df.groupBy(
    sf.col('col2'),
    sf.col('col1'),
).agg(sf.sum(sf.col('col3'))).show()



df.createOrReplaceTempView("table")

group_by_col12 = spark.sql(
    """ (
        SELECT col1, col2, sum(col3) FROM table
        group by col1, col2
    )
    """
).show()

group_by_col21 = spark.sql(
    """ (
        SELECT col1, col2, sum(col3), max(col3) FROM table
        group by col2, col1
    )
    """
).show()




