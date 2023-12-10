from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf


if __name__ == '__main__':

    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .getOrCreate()
    )

    # spark.sparkContext.setLogLevel("INFO")

    first_schema = st.StructType([
        st.StructField("id", st.IntegerType(), True),
        st.StructField("postalcode", st.StringType(), True),
        st.StructField("col1", st.StringType(), True),
        st.StructField("col2", st.StringType(), True),
    ])

    second_schema = st.StructType([
        st.StructField("id", st.IntegerType(), True),
        st.StructField("col1", st.StringType(), True),
        st.StructField("col2", st.StringType(), True),
    ])

    first_df = spark.createDataFrame(
        [
            (11, "n/a", "abcdfskfj", "23487jhk"),
            (12, "123", "abcdfskfj", "23487jhk"),
            (13, "1234", "abcdfskfj", "23487jhk"),
            (14, "12345", "abcdfskfj", "23487jhk"),
            (15, "123456", "abcdfskfj", "23487jhk"),
            (16, "1234567", "abcdfskfj", "23487jhk"),
            (17, "12345678", "abcdfskfj", "23487jhk"),
            (18, "123456789", "abcdfskfj", "23487jhk"),
        ],
        schema=first_schema
    )

    second_df = spark.createDataFrame(

        [
            (21, "abcdfskfj", "23487jhk"),
            (22, "abcdfskfj", "23487jhk"),
            (23, "abcdfskfj", "23487jhk"),
            (24, "abcdfskfj", "23487jhk"),
            (25, "abcdfskfj", "23487jhk"),
            (26, "abcdfskfj", "23487jhk"),
            (27, "abcdfskfj", "23487jhk"),
            (28, "abcdfskfj", "23487jhk"),
        ],

        schema=second_schema
    ).withColumn(
        "postalcode",
        sf.lit(None)
    )

    first_df.show()
    second_df.show()

    union_df = first_df.unionByName(
        second_df
    )

    union_df.show()
