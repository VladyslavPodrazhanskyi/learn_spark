import pytest
import pyspark.sql.functions as F
import pyspark.sql.types as T
from adl_iafa_glb_etl.transformations import AddDateTimeColumns




class TestAddDateTimeColumns:
    """
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
            'test_data',
            'expected_data'
        ),
        [
            (
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
        add_date_time = AddDateTimeColumns()
        actual_df = add_date_time.transform(test_df)

        # save expected data as dataframe
        expected_df = create_dataframe(expected_data)

        assert_dataframes(
            expected_df=expected_df,
            actual_df=actual_df,
        )
