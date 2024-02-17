"""
csv file - test source_data
schema

read csv with schema

write json

read json as df with schema ( otherwise time to string)

"""


from pprint import pprint
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from my_code import ROOT

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


# schema = T.StructType([
#     T.StructField('strcol1', T.StringType()),
#     T.StructField('longcol2', T.LongType()),
#     T.StructField('timecol3', T.TimestampType()),
# ])

schema = T.StructType(List(
    T.StructField('strcol1', T.StringType()),
    T.StructField('longcol2', T.LongType()),
    T.StructField('timecol3', T.TimestampType()),
))


df_csv = (
    spark
    .read
    .option("header", True)
    .schema(schema)
    .csv(f"{ROOT}/code/my_practice/nested_schema/data1.csv")
)


df_csv.show()
df_csv.printSchema()

# write df from csv to json format
(
    df_csv
    .write
    .mode("overwrite")
    .json(f"{ROOT}/code/my_practice/nested_schema/json_dir")
)

# read df from json file:
df_json = (
    spark
    .read
    .schema(schema)
    .json(f"{ROOT}/code/my_practice/nested_schema/json_dir/*.json")
)

df_json.show()
df_json.printSchema()

sc = df_json.schema
print(type(sc), sc)

# df_without_schema.printSchema()
# df_without_schema.show()
#
# schema = T.StructType([
#     T.StructField('string_column1', T.StringType()),
#     T.StructField('string_column2', T.StringType()),
#     T.StructField('string_column3', T.DoubleType()),
# ])
#
# df_with_schema = spark.read.format("com.crealytics.spark.excel") \
#     .option("header", True) \
#     .schema(schema) \
#     .load(f'{ROOT}/source_data/test_excel_file.xlsx')
#
# df_with_schema.printSchema()
# df_with_schema.show()



#
#
# df_csv_without_schema = spark.format("com.crealytics.spark.excel") \
#     .option("header", True) \
#     .option("inferSchema", False) \
#     .load(f'{ROOT}/source_data/test_excel_file.xlsx')
