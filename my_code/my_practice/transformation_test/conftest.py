import pytest

from spark_fixtures import (
    spark,
    create_dataframe,
    assert_dataframes,
    assert_schemas,
    read_dataframe,
    read_table_dataframe,
)

test_spark = pytest.fixture(spark, scope='session')
create_dataframe = pytest.fixture(create_dataframe)
assert_dataframes = pytest.fixture(assert_dataframes)
assert_schemas = pytest.fixture(assert_schemas)
read_dataframe = pytest.fixture(read_dataframe)
read_table_dataframe = pytest.fixture(read_table_dataframe)

