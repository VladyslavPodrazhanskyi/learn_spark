import unittest
from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

from my_code.my_practice.practice_testing.transformation import transform_data


class SparkETLTestsUnit(unittest.TestCase):
    """Test suite for transformation in etl_parse_tags.py
    """

    def setUp(self):
        """Start Spark
        """
        self.spark = SparkSession.builder.master("local[2]").getOrCreate()

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        #  Create data samples

        schema1 = st.StructType([
            st.StructField('col11', st.IntegerType(), True),
            st.StructField('col2', st.DoubleType(), True),
            st.StructField('col13', st.StringType(), True),
        ])

        schema2 = st.StructType([
            st.StructField('col21', st.IntegerType(), True),
            st.StructField('col2', st.DoubleType(), True),
            st.StructField('col23', st.StringType(), True),
        ])

        df1 = self.spark.createDataFrame(
            [
                (1, 2.0, 'AD12'),
                (1, 2.28449, ''),
                (1, 4.000000000001, 'BP3')
            ],
            schema1
        )

        df2 = self.spark.createDataFrame(
            [
                (1, 2.0, 'AD12'),
                (1, 2.28449, ''),
                (1, 4.000000000001, 'skf')
            ],
            schema2
            # ("col21", "col2", "col23")
        )

        #  transform test_df
        tranformed_df = transform_data(df1, df2, 'col2')

        expected_schema = st.StructType([
            st.StructField('col2', st.DoubleType(), True),
            st.StructField('col11', st.IntegerType(), True),
            st.StructField('col13', st.StringType(), True),
            st.StructField('col21', st.IntegerType(), True),
            st.StructField('col23', st.StringType(), True),
        ])

        expected_data = [
            (2.0, 1, 'AD12', 1, 'AD12'),
            (2.28449, 1, '', 1, ''),
            (4.000000000001, 1, 'BP3', 1, 'skf')
        ]

        # expected_df from expected data
        expected_df = self.spark.createDataFrame(
            expected_data,
            expected_schema
        )

        # Check equality of expected_df and transformed_df
        self.assertEqual(expected_df.schema, tranformed_df.schema)
        self.assertEqual(expected_df.collect(), tranformed_df.collect())


if __name__ == '__main__':
    unittest.main()
