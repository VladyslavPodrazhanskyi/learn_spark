import pytest
from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

from my_code.my_practice.practice_testing.transformation import transform_data


class TestSparkETLPyTests:
    """
    Test suite for transformation in etl_parse_tags.py
    1. Create @pytest.mark.parametrize to set test_data and expected data
    1.1. tuple with name of parameters for test
    1.2. list of tuples with data for every test case

    2. def test_run(self, arguments) define test instance method:
    2.1. optional mocker to mock some spark function
    for example:
    # mock current_timestamp
    mocker.patch(
        'pyspark.sql.functions.current_timestamp',
        lambda: F.lit('2010-10-10 10:10:10')
    )
    2.2. add test_data and expected_data to arguments
    2.3. add fixtures - functions registered as fixturesthe
    to reuse of setup and teardown logic across multiple tests.
    functions created in dir fixtures,
    imported and registered as fixtures in the conftest.py
    Fixtures play role set up and tear down methods for unittest
    """

    @pytest.mark.parametrize(
        (
            'test_ldf_data',
            'test_rdf_data',
            'expected_data'
        ),
        [
            (
                # test_ldf_data
                {
                    st.StructField('col11', st.IntegerType()): [
                        1,
                        1,
                        1
                    ],
                    st.StructField('col2', st.DoubleType()): [
                        2.0,
                        2.28449,
                        4.000000000001
                    ],
                    st.StructField('col13', st.StringType()): [
                        'AD12',
                        '',
                        'BP3',
                    ],
                },
                # test_rdf_data
                {
                    st.StructField('col21', st.IntegerType()): [
                        1,
                        1,
                        1
                    ],
                    st.StructField('col2', st.DoubleType()): [
                        2.0,
                        2.28449,
                        4.000000000001
                    ],
                    st.StructField('col23', st.StringType()): [
                        'AD12',
                        '',
                        'skf',
                    ],
                },
                # expected_data
                {
                    st.StructField('col2', st.DoubleType()): [
                        2.0,
                        2.28449,
                        4.000000000001
                    ],
                    st.StructField('col11', st.IntegerType()): [
                        1,
                        1,
                        1
                    ],
                    st.StructField('col13', st.StringType()): [
                        'AD12',
                        '',
                        'BP3'
                    ],
                    st.StructField('col21', st.IntegerType()): [
                        1,
                        1,
                        1
                    ],
                    st.StructField('col23', st.StringType()): [
                        'AD12',
                        '',
                        'skf',
                    ],
                }
            )
        ]
    )
    def test_run(
        self,
        # mocker
        test_ldf_data,
        test_rdf_data,
        expected_data,
        assert_dataframes,
        create_dataframe
    ):
        # save test data as dataframe
        ldf = create_dataframe(test_ldf_data)
        rdf = create_dataframe(test_rdf_data)

        # transform test data
        actual_df = transform_data(ldf, rdf, 'col2')

        # save expected data as dataframe
        expected_df = create_dataframe(expected_data)

        assert_dataframes(
            expected_df=expected_df,
            actual_df=actual_df
        )
