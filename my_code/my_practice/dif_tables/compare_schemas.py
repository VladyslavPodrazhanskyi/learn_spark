from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

from my_code import ROOT

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

# path1 = f'{ROOT}/my_code/my_practice/dif_tables/source/current_ground_truth/year=2022/'
path1 = f'{ROOT}/my_code/my_practice/dif_tables/source/ground_truth_1304/'
path2 = f'{ROOT}/my_code/my_practice/dif_tables/source/renamed_ground_truth/'

df1 = spark.read.parquet(path1)
df2 = spark.read.parquet(path2)

df1.show()
df2.show()


set_fields1 = {field.simpleString() for field in df1.schema.fields}
set_fields2 = {field.simpleString() for field in df2.schema.fields}

first_minus_second = sorted(list(set_fields1 - set_fields2))
second_minus_first = sorted(list(set_fields2 - set_fields1))

print(f'first_minus_second: {first_minus_second}')
print(f'second_minus_first: {second_minus_first}')

# print(set_fields1)

# df1.select(
#     sf.col('month'),
#     sf.col('day')
# ).show()


#
# def factory(actual_schema: T.StructType, expected_schema: T.StructType) -> None:
#     expected_schema = [field.simpleString() for field in expected_schema.fields]
#     actual_schema = [field.simpleString() for field in actual_schema.fields]
#
#     assert sorted(expected_schema) == sorted(actual_schema)
