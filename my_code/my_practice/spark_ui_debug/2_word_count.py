from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from code import ROOT

# http://192.168.100.3:4040/


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config('spark.driver.memory', '16g')
    .getOrCreate()
)

print(spark.conf.get('spark.driver.memory'))

#
results = (
    spark.read.text(f"{ROOT}/source_data/Gutenberg/*.txt")
    .select(sf.split(sf.col("value"), " ").alias("line"))
    .select(sf.explode(sf.col("line")).alias("word"))
    .select(sf.lower(sf.col("word")).alias("word"))
    .select(sf.regexp_extract(sf.col("word"), "[a-z']+", 0).alias("word"))  # sign (+) matches one or more occurrences of the one-character regular expression.
    .where(sf.col("word") != "")
    .groupby(sf.col("word"))
    .count()
    .orderBy(sf.col('count').desc())
    .cache()
)
#
#
results.explain(mode='extended')
#
# results.rdd.getNumPartitions()