from datetime import datetime

import pytest

from pyspark.sql import types as T

from adl_iafa_glb_etl.transformations import UpsertTransformation


class TestUpsertTransformation:
    _partition_column_name: str = 'partition'
    _key_column_name: str = 'key'

    _ingestion_time_column_name: str = 'ingestion_time'

    _value_column_name: str = 'value'

    @pytest.mark.parametrize(
        (
            'test_data',

            'expected_data',
        ),
        [
            (
                {
                    'base': {
                        T.StructField(_partition_column_name, T.StringType()): [1],
                        T.StructField(_key_column_name, T.StringType()): [1],
                        T.StructField(_value_column_name, T.StringType()): [1],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 10, 10, 10),
                        ],
                    },
                    'inc': {
                        T.StructField(_partition_column_name, T.StringType()): [2],
                        T.StructField(_key_column_name, T.StringType()): [2],
                        T.StructField(_value_column_name, T.StringType()): [2],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 10, 10, 10),
                        ],
                    },
                },

                {
                    T.StructField(_partition_column_name, T.StringType()): [2],
                    T.StructField(_key_column_name, T.StringType()): [2],
                    T.StructField(_value_column_name, T.StringType()): [2],
                    T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                        datetime(2020, 10, 10, 10, 10, 10),
                    ],
                },
            ),
            (
                {
                    'base': {
                        T.StructField(_partition_column_name, T.StringType()): [1],
                        T.StructField(_key_column_name, T.StringType()): [1],
                        T.StructField(_value_column_name, T.StringType()): [1],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 10, 10, 10),
                        ],
                    },
                    'inc': {
                        T.StructField(_partition_column_name, T.StringType()): [1],
                        T.StructField(_key_column_name, T.StringType()): [3],
                        T.StructField(_value_column_name, T.StringType()): [2],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 11, 11, 11),
                        ],
                    },
                },

                {
                    T.StructField(_partition_column_name, T.StringType()): [1, 1],
                    T.StructField(_key_column_name, T.StringType()): [1, 3],
                    T.StructField(_value_column_name, T.StringType()): [1, 2],
                    T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                        datetime(2020, 10, 10, 10, 10, 10),
                        datetime(2020, 10, 10, 11, 11, 11),
                    ],
                },
            ),
            (
                {
                    'base': {
                        T.StructField(_partition_column_name, T.StringType()): [1],
                        T.StructField(_key_column_name, T.StringType()): [1],
                        T.StructField(_value_column_name, T.StringType()): [1],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 10, 10, 10),
                        ],
                    },
                    'inc': {
                        T.StructField(_partition_column_name, T.StringType()): [1],
                        T.StructField(_key_column_name, T.StringType()): [1],
                        T.StructField(_value_column_name, T.StringType()): [2],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 11, 11, 11),
                        ],
                    },
                },

                {
                    T.StructField(_partition_column_name, T.StringType()): [1],
                    T.StructField(_key_column_name, T.StringType()): [1],
                    T.StructField(_value_column_name, T.StringType()): [2],
                    T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                        datetime(2020, 10, 10, 11, 11, 11),
                    ],
                },
            ),
            (
                {
                    'base': {
                        T.StructField(_partition_column_name, T.StringType()): [1],
                        T.StructField(_key_column_name, T.StringType()): [1],
                        T.StructField(_value_column_name, T.StringType()): [1],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 10, 10, 10),
                        ],
                    },
                    'inc': {
                        T.StructField(_partition_column_name, T.StringType()): [1],
                        T.StructField(_key_column_name, T.StringType()): [1],
                        T.StructField(_value_column_name, T.StringType()): [1],
                        T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                            datetime(2020, 10, 10, 10, 10, 10),
                        ],
                    },
                },

                {
                    T.StructField(_partition_column_name, T.StringType()): [1],
                    T.StructField(_key_column_name, T.StringType()): [1],
                    T.StructField(_value_column_name, T.StringType()): [1],
                    T.StructField(_ingestion_time_column_name, T.TimestampType()): [
                        datetime(2020, 10, 10, 10, 10, 10),
                    ],
                },
            ),
        ],
    )
    def test_transform(
        self,
        test_data,
        expected_data,

        create_dataframe,
        assert_dataframes,
    ):
        transformation = UpsertTransformation(
            create_dataframe(test_data['base']),
            [self._partition_column_name],
            [self._key_column_name],
            self._ingestion_time_column_name,
        )

        actual_df = transformation.transform(
            create_dataframe(test_data['inc']),
        )

        assert_dataframes(
            actual_df=actual_df,
            expected_df=create_dataframe(expected_data),
        )
