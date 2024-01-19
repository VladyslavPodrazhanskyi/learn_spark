import time
from typing import List, Tuple
from my_code import ROOT


from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


df = (
    spark
    .read
    .option('header', 'true')
    .csv(f"{ROOT}/source_data/retail-data/all/online-retail-dataset.csv")
    # .repartition(2)
    .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")
    .groupBy("is_glass")
    .count()
)


print(df.collect())
# print(df.count())
# df.show()
# df.collect()


# (
#     spark
#     .read
#     .option('header', 'true')
#     .csv(f"{ROOT}/source_data/retail-data/all/online-retail-dataset.csv")
#     .repartition(2)
#     .selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")
#     .groupBy("is_glass")
#     .count()
#     .collect()
# )

time.sleep(600)
spark.stop()
