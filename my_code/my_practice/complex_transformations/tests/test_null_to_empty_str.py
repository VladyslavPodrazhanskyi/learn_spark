import pytest
import pyspark.sql.types as T

from adl_iafa_glb_etl.transformations import RenameColumns
from adl_iafa_glb_etl.transformations._null_to_empty_str import NullToEmptyStrTransformation


class TestRenameColumns:
    @pytest.mark.parametrize(
        (
            'columns',
            'test_data',
            'expected_data'
        ),
        [
            (
                # 'columns'
                [
                    "column1"
                ],
                # 'test_data'
                {
                    T.StructField('column1', T.StringType()): [
                        None,
                        '1C',
                        '  ',
                    ],
                    T.StructField('column2', T.StringType()): [
                        None,
                        '2Z',
                        ' ',
                    ],
                },
                # 'expected_data'
                {
                    T.StructField('column1', T.StringType()): [
                        '',
                        '1C',
                        ''
                    ],
                    T.StructField('column2', T.StringType()): [
                        None,
                        '2Z',
                        ' '
                    ],
                },
            )
        ]
    )
    def test_run(
        self,
        columns,
        test_data,
        expected_data,

        assert_dataframes,
        create_dataframe

    ):
        # save test data as dataframe
        test_df = create_dataframe(test_data)

        # transform test_df
        transformation = NullToEmptyStrTransformation(columns)
        actual_df = transformation.transform(test_df)

        # save expected data as dataframe
        expected_df = create_dataframe(expected_data)

        assert_dataframes(
            expected_df=expected_df,
            actual_df=actual_df,
        )
