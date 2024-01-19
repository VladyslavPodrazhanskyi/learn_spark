from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf


def transform_data(left_df: DataFrame, right_df: DataFrame, common_col: str) -> DataFrame:
    return left_df.join(
        right_df,
        on=common_col,
        how='inner'
    )


if __name__ == '__main__':
    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .getOrCreate()
    )

    schema1 = st.StructType([
        st.StructField('col11', st.IntegerType(), True),
        st.StructField('col12', st.DoubleType(), True),
        st.StructField('col13', st.StringType(), True),
    ])

    schema2 = st.StructType([
        st.StructField('col21', st.IntegerType(), True),
        st.StructField('col22', st.DoubleType(), True),
        st.StructField('col23', st.StringType(), True),
    ])

    df1 = spark.createDataFrame(
        [
            (1, 2.0, 'AD12'),
            (1, 2.28449, ''),
            (1, 4.000000000001, 'BP3')
        ],
        schema1
    )

    df1 = df1.withColumnRenamed('col12', 'col2')

    df2 = spark.createDataFrame(
        [
            (1, 2.0, 'AD12'),
            (1, 2.28449, ''),
            (1, 4.000000000001, 'skf')
        ],
        schema2
        # ("col21", "col22", "col23")
    )

    df2 = df2.withColumnRenamed('col22', 'col2')

    joined_df = transform_data(df1, df2, 'col2')

    joined_df.show()
    joined_df.printSchema()
