import pytest

from my_code.my_practice.practice_testing.fixtures.spark_fixture import (
    spark,
    create_dataframe,
    assert_dataframes,
    assert_schemas
)

fake_spark = pytest.fixture(spark, scope='session')
create_dataframe = pytest.fixture(create_dataframe)
assert_dataframes = pytest.fixture(assert_dataframes)
assert_schemas = pytest.fixture(assert_schemas)

