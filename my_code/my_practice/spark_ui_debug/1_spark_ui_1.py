from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from my_code import ROOT

# http://192.168.100.3:4040/


spark = (
    SparkSession
        .builder
        .master("local[*]")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

df = (
    spark.read
        .option("header", "true")
        .csv("{ROOT}/source_data/retail-source_data/all/online-retail-dataset.csv".format(ROOT=ROOT))
)

df.groupBy(
    sf.col('Country')
).agg(
    sf.avg(
        sf.col('UnitPrice')
    )
).show()
0


print(df.rdd.getNumPartitions())

(
    spark.read
        .option("header", "true")
        .csv("{ROOT}/source_data/retail-source_data/all/online-retail-dataset.csv".format(ROOT=ROOT))
        .repartition(2)
        .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")
        .groupBy("is_glass")
        .count()
        .collect()
)
