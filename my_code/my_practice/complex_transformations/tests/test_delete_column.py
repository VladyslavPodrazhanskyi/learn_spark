import pytest
import pyspark.sql.types as T

from adl_iafa_glb_etl.transformations import RemoveServiceColumns


class TestRemoveServiceColumns:
    @pytest.mark.parametrize(
        (
            'columns_to_delete',
            'test_data',
            'expected_data'
        ),
        [
            (  # 'columns_to_delete'
                ['column2', ],

                # 'test_data'
                {
                    T.StructField('column1', T.StringType()): [
                        '1B',
                        '1C',
                        '1D'
                    ],
                    T.StructField('column2', T.StringType()): [
                        '2A',
                        '2Z',
                        '2cd'
                    ],
                    T.StructField('column3', T.IntegerType()): [
                        11,
                        278,
                        45
                    ],
                },

                # 'expected_data'
                {
                    T.StructField('column1', T.StringType()): [
                        '1B',
                        '1C',
                        '1D'
                    ],
                    T.StructField('column3', T.IntegerType()): [
                        11,
                        278,
                        45
                    ]
                }
            )
        ]
    )
    def test_run(
        self,
        columns_to_delete,
        test_data,
        expected_data,

        assert_dataframes,
        create_dataframe

    ):
        # save test data as dataframe
        test_df = create_dataframe(test_data)

        # delete columns from test_df
        remove_service = RemoveServiceColumns(columns_to_delete)
        actual_df = remove_service.transform(test_df)

        # save expected data as dataframe
        expected_df = create_dataframe(expected_data)

        assert_dataframes(
            expected_df=expected_df,
            actual_df=actual_df,
        )
