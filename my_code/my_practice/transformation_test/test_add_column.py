import pytest
import pyspark.sql.functions as F
import pyspark.sql.types as T

from add_time_col import add_date_time_cols


class TestAddDateTimeColumns:
    @pytest.mark.parametrize(
        (
            'test_data',
            'expected_data'
        ),
        [
            (
                # 'test_data'
                {
                    T.StructField('stable_str_column', T.StringType()): [
                        '1B',
                        '1C',
                        '17D'
                    ],
                    T.StructField('stable_int_column', T.IntegerType()): [
                        11,
                        278,
                        45
                    ],
                },
                # 'expected_data'
                {
                    T.StructField('stable_str_column', T.StringType()): [
                        '1B',
                        '1C',
                        '17D'
                    ],
                    T.StructField('stable_int_column', T.IntegerType()): [
                        11,
                        278,
                        45
                    ],
                    T.StructField('ingestion_date', T.StringType()): [
                        '2010-10-10',
                        '2010-10-10',
                        '2010-10-10',
                    ],
                    T.StructField('ingestion_time', T.StringType()): [
                        '10:10:10',
                        '10:10:10',
                        '10:10:10'
                    ],
                },
            )
        ]
    )
    def test_run(
        self,
        mocker,
        test_data,
        expected_data,

        assert_dataframes,
        create_dataframe,
    ):
        # mock current_timestamp
        mocker.patch(
            'pyspark.sql.functions.current_timestamp',
            lambda: F.lit('2010-10-10 10:10:10')
        )

        # save test data as dataframe
        test_df = create_dataframe(test_data)

        # transform test_df
        actual_df = add_date_time_cols(test_df)

        # save expected data as dataframe
        expected_df = create_dataframe(expected_data)

        assert_dataframes(
            expected_df=expected_df,
            actual_df=actual_df,
        )


