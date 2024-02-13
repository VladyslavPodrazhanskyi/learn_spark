import pytest

from pyspark.sql import types as T

from adl_iafa_glb_etl.transformations import UnionTransformation


class TestUnionTransformation:
    @pytest.mark.parametrize(
        (
            'test_data',
            'expected_data',
        ),
        [
            (
                {
                    'acc': [
                        {
                            T.StructField('column', T.IntegerType()): [1],
                        },
                        {
                            T.StructField('column', T.IntegerType()): [2],
                        },
                    ],
                    'inc': {
                        T.StructField('column', T.IntegerType()): [3],
                    }
                },

                {
                    T.StructField('column', T.IntegerType()): [1, 2, 3],
                },
            ),
            (
                {
                    'acc': [
                        {
                            T.StructField('column', T.IntegerType()): [1],
                        },
                        {
                            T.StructField('some_column', T.IntegerType()): [2],
                        },
                    ],
                    'inc': {
                        T.StructField('column', T.IntegerType()): [3],
                    }
                },

                {
                    T.StructField('column', T.IntegerType()): [3, 1, None],
                    T.StructField('some_column', T.IntegerType()): [None, None, 2],
                },
            ),
            (
                {
                    'acc': [
                        {
                            T.StructField('column', T.IntegerType()): [1],
                        },
                        {
                            T.StructField('some_column', T.IntegerType()): [2],
                        },
                    ],
                    'inc': {
                        T.StructField('another_column', T.IntegerType()): [3],
                    }
                },

                {
                    T.StructField('another_column', T.IntegerType()): [3, None, None],
                    T.StructField('column', T.IntegerType()): [None, 1, None],
                    T.StructField('some_column', T.IntegerType()): [None, None, 2],
                },
            )
        ],
    )
    def test_transform(
        self,
        test_data,
        expected_data,

        create_dataframe,
        assert_dataframes,
    ):
        transformation = UnionTransformation([
            create_dataframe(acc)
            for acc in test_data['acc']
        ])

        actual_df = transformation.transform(
            create_dataframe(test_data['inc']),
        )

        assert_dataframes(
            actual_df=actual_df,
            expected_df=create_dataframe(expected_data),
        )
