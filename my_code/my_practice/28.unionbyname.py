from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf


def process_postalcode(input_df: DataFrame) -> DataFrame:

    process_postalcode_df = input_df.withColumn(
        "processed_postalcode",
        sf.when(
            (sf.length(sf.col("postalcode")) == 3)
            & (sf.col("postalcode") != 'n/a'),
            sf.concat(sf.lit('00'), sf.col("postalcode"))
        ).when(
            sf.length(sf.col("postalcode")) == 4,
            sf.concat(sf.lit('0'), sf.col("postalcode"))
        ).when(
            sf.length(sf.col("postalcode")) == 8,
            sf.concat(sf.lit('0'), sf.substring(sf.col("postalcode"), 1, 4))
        ).when(
            sf.length(sf.col("postalcode")) == 9,
            sf.substring(sf.col("postalcode"), 1, 5)
        ).otherwise(
            sf.col("postalcode")
        )
    )
    return process_postalcode_df


if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .getOrCreate()
    )

    # spark.sparkContext.setLogLevel("INFO")

    schema = st.StructType([
        st.StructField("id", st.IntegerType(), True),
        st.StructField("postalcode", st.StringType(), True),
        st.StructField("col2", st.StringType(), True),
        st.StructField("col3", st.StringType(), True),
    ])

    cur_df = spark.createDataFrame(
        [
            (1, "n/a", "abcdfskfj", "23487jhk"),
            (2, "123", "abcdfskfj", "23487jhk"),
            (3, "1234", "abcdfskfj", "23487jhk"),
            (4, "12345", "abcdfskfj", "23487jhk"),
            (5, "123456", "abcdfskfj", "23487jhk"),
            (6, "1234567", "abcdfskfj", "23487jhk"),
            (7, "12345678", "abcdfskfj", "23487jhk"),
            (8, "123456789", "abcdfskfj", "23487jhk"),
        ],
        schema=schema
    )

    processed_df = process_postalcode(cur_df)

    processed_df.show()


