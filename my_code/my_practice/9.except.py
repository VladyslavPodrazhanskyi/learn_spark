from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


df1 = spark.createDataFrame([(2, 5, 7), (1, 2, 4), (1, 4, 6), (2, 5, 7)], ("col1", "col2", "col3"))
df2 = spark.createDataFrame([(2, 5, 7), (1, 2, 4), (1, 4, 6), (1, 2, 3)], ("col1", "col2", "col3"))


df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

print('table_1')
spark.sql("SELECT * FROM table1").show()
print('table_2')
spark.sql("SELECT * FROM table2").show()

print("except")

spark.sql(
    """ 
    (SELECT * FROM table1
    EXCEPT
    SELECT * FROM table2)             
    """
).show()

print("except all")

spark.sql(
    """ 
    (SELECT * FROM table1
    EXCEPT ALL
    SELECT * FROM table2)      
    """
).show()





