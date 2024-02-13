import pytest
import pyspark.sql.types as T
from adl_iafa_glb_etl.transformations import DropDuplicates


class TestDropDuplicates:
    @pytest.mark.parametrize(
        (
            'pk_columns',
            'test_data',
            'expected_data'
        ),
        [
            (
                # 'pk_columns'
                [
                    'column1',
                ],

                # 'test_data'
                {
                    T.StructField('column1', T.StringType()): [
                        '1B',
                        '1C',
                        '1B',
                    ],
                    T.StructField('column2', T.StringType()): [
                        '2A',
                        '2Z',
                        '2A',
                    ],
                    T.StructField('column3', T.IntegerType()): [
                        11,
                        278,
                        11
                    ],
                },
                # 'expected_data'
                {
                    T.StructField('column1', T.StringType()): [
                        '1B',
                        '1C',
                    ],
                    T.StructField('column2', T.StringType()): [
                        '2A',
                        '2Z',

                    ],
                    T.StructField('column3', T.IntegerType()): [
                        11,
                        278,
                    ],
                },
            )
        ]
    )
    def test_run(
        self,
        pk_columns,
        test_data,
        expected_data,

        assert_dataframes,
        create_dataframe

    ):
        # save test data as dataframe
        test_df = create_dataframe(test_data)

        # transform test_df
        drop_duplicates = DropDuplicates(pk_columns)
        actual_df = drop_duplicates.transform(test_df)

        # save expected data as dataframe
        expected_df = create_dataframe(expected_data)

        assert_dataframes(
            expected_df=expected_df,
            actual_df=actual_df,
        )
