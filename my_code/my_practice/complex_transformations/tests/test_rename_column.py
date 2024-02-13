import pytest
import pyspark.sql.types as T

from adl_iafa_glb_etl.transformations import RenameColumns


class TestRenameColumns:
    @pytest.mark.parametrize(
        (
            'mapping',
            'test_data',
            'expected_data'
        ),
        [
            (
                # 'mapping'
                {
                    "column1": "column1_new",
                    "colUmn2": "column2_new"
                },
                # 'test_data'
                {
                    T.StructField('column1', T.StringType()): [
                        '1B',
                        '1C',
                        '1D',
                    ],
                    T.StructField('colUmn2', T.StringType()): [
                        '2A',
                        '2Z',
                        '2cd',
                    ],
                    T.StructField('cOLuMn3', T.IntegerType()): [
                        11,
                        278,
                        45
                    ],
                },
                # 'expected_data'
                {
                    T.StructField('column1_new', T.StringType()): [
                        '1B',
                        '1C',
                        '1D'
                    ],
                    T.StructField('column2_new', T.StringType()): [
                        '2A',
                        '2Z',
                        '2cd'
                    ],
                    T.StructField('cOLuMn3', T.IntegerType()): [
                        11,
                        278,
                        45
                    ],
                },
            )
        ]
    )
    def test_run(
        self,
        mapping,
        test_data,
        expected_data,

        assert_dataframes,
        create_dataframe

    ):
        # save test data as dataframe
        test_df = create_dataframe(test_data)

        # transform test_df
        rename_columns = RenameColumns(mapping)
        actual_df = rename_columns.transform(test_df)

        # save expected data as dataframe
        expected_df = create_dataframe(expected_data)

        assert_dataframes(
            expected_df=expected_df,
            actual_df=actual_df,
        )
