from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


df1 = spark.createDataFrame([(1, 'val1'), (2, 'val2'), (3, 'val3'), (4, 'val4'), (5, 'val5'), (6, 'val6')], ("id", "test_col"))
df2 = spark.createDataFrame([(1, 'val1'), (2, 'val2'), (3, 'val3'), (4, 'val4'), (5, 'val5'), (6, 'val7')], ("id", "test_col"))


df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

spark.sql("SELECT * FROM table1").show()
spark.sql("SELECT * FROM table2").show()

print("inner join")

spark.sql(
    """ 
    (SELECT * 
    FROM table1   
    JOIN table2
    on table1.id = table2.id
    where table1.test_col != table2.test_col
    )              
    """
).show()






