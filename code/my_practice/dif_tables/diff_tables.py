from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


df1 = spark.createDataFrame([(1, 2, 3), (1, 2, 4), (1, 2, 5), (1, 2, 6), (1, 2, 7), (1, 2, 7)], ("col1", "col2", "col3"))
df2 = spark.createDataFrame([(1, 2, 3), (1, 2, 4), (1, 2, 5), (1, 2, 6), (1, 2, 8)], ("col1", "col2", "col3"))


df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

spark.sql("SELECT * FROM table1").show()
spark.sql("SELECT * FROM table2").show()

print("except all")

spark.sql(
    """ 
    (SELECT * FROM table1
    EXCEPT ALL
    SELECT * FROM table2) 
    UNION ALL 
    (SELECT * FROM table2
    EXCEPT ALL
    SELECT * FROM table1)              
    """
).show()

print("intersect")

spark.sql(
    """
    (SELECT * FROM table1
    UNION ALL
    SELECT * FROM table2)
    EXCEPT 
    (SELECT * FROM table2
    INTERSECT
    SELECT * FROM table1)
    """
).show()





