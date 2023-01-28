import os
from typing import List, Any, Dict

from pyspark.sql import SparkSession, DataFrame, types as T


def spark():
    session = (
        SparkSession
        .builder
        .master('local[2]')
        .appName('unit-test')
        .enableHiveSupport()
        .getOrCreate()
    )

    yield session

    session.stop()


def create_dataframe(test_spark: SparkSession):
    """To create spark DataFrame based on columns and it's values."""

    def factory(fields: Dict[T.StructField, List[Any]]) -> DataFrame:
        return test_spark.createDataFrame(
            list(zip(*fields.values())),
            schema=T.StructType(list(fields.keys()))
        )

    return factory


def assert_dataframes(assert_schemas):
    """To compare two spark DataFrames."""

    def factory(*, expected_df: DataFrame, actual_df: DataFrame) -> None:
        assert_schemas(actual_df.schema, expected_df.schema)

        sorted_expected_df = (expected_df
                              .select(actual_df.columns)
                              .orderBy(actual_df.columns)
                              .collect())

        sorted_actual_df = (actual_df
                            .orderBy(actual_df.columns)
                            .collect())

        assert sorted_expected_df == sorted_actual_df

    return factory


def assert_schemas():
    def factory(actual_schema: T.StructType, expected_schema: T.StructType) -> None:
        expected_schema = [field.simpleString() for field in expected_schema.fields]
        actual_schema = [field.simpleString() for field in actual_schema.fields]

        assert sorted(expected_schema) == sorted(actual_schema)

    return factory


def read_dataframe(test_spark):
    def factory(bucket_name: str, file_key: str) -> DataFrame:
        return test_spark.read.parquet(os.path.join(bucket_name, file_key))

    return factory


def read_table_dataframe(test_spark):
    def factory(db_name: str, table_name: str) -> DataFrame:
        return test_spark.table(
            '{db_name}.{table_name}'.format(
                db_name=db_name,
                table_name=table_name,
            ),
        )

    return factory





