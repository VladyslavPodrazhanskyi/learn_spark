from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    .getOrCreate()
)

spark.sparkContext.setCheckpointDir(f'{ROOT}/checkpoints')


df = (
    spark
    .read
    .option("header", "true")
    .option('sep', ';')
    .option("inferSchema", "true")
    .csv(f"{ROOT}/source_data/kaggle/*.csv")
)

df.checkpoint()
df.printSchema()
df.show()
print(df.count())