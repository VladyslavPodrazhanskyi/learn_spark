import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st


@sf.udf(returnType=st.DoubleType())
def cubeUDF(x):
    return None if x is None else x ** 3


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    schema = st.StructType([
        st.StructField("stable_column", st.StringType(), True),
        st.StructField("currency", st.StringType(), True),
        st.StructField("cur_value", st.DoubleType(), True),
    ])

    df = spark.createDataFrame(
        [
            ('value', 'EUR', 1.19),
            ('value', 'UAH', 32.54),
            ('value', 'EUR', 1.0),
            ('value', 'UAH', None),
            ('value', 'UAH', 32.54),
        ],
        schema=schema
    )

    df = df.withColumn(
        'cube',
        cubeUDF(sf.col("cur_value"))
    )

    df.show()
    print(df.count())
    print(df.schema)

    # df = df.withColumn(
    #     "count", squareUDF(sf.col('count'))
    # )

    # df = df.withColumn(
    #     "square_count", squareUDF(sf.col('count'))
    # ).withColumn(
    #     "cube_count",
    #     cubeUDF(sf.col('count'))
    # )

    # df = df.select("count", squareUDF(sf.col('count')).alias('selected_square'))
    # df.show()

    spark.stop()



