import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st


from my_code import ROOT


def square(x):
    return x ** 2


squareUDF = sf.udf(lambda x: square(x), st.LongType())


@sf.udf(returnType=st.LongType())
def cubeUDF(x):
    return x ** 3


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.read.format("json").load(f"{ROOT}/source_data/flight-data/json/")

    df.show()
    print(df.count())
    print(df.schema)

    df = df.withColumn(
        "count", squareUDF(sf.col('count'))
    )

    # df = df.withColumn(
    #     "square_count", squareUDF(sf.col('count'))
    # ).withColumn(
    #     "cube_count",
    #     cubeUDF(sf.col('count'))
    # )

    # df = df.select("count", squareUDF(sf.col('count')).alias('selected_square'))
    df.show()

    spark.stop()
