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
from schemas import full_schema, part_schema

from code import ROOT

spark = (
    SparkSession
        .builder
        .master("local[*]")
        .getOrCreate()
)

spark.sql()



# schema = T.StructType([
#     T.StructField('strcol1', T.StringType()),
#     T.StructField('longcol2', T.LongType()),
#     T.StructField('timecol3', T.TimestampType()),
# ])


#
# #
# df_csv = (
#     spark
#     .read
#     .option("header", True)
#     .schema(part_schema)
#     .csv(f"{ROOT}/code/my_practice/nested_schema/corrected_concur_report.csv")
# )
# #
# #
# print('df_csv')
# df_csv.show()
# df_csv.printSchema()
#
# # write df from csv to json format
# (
#     df_csv
#     .write
#     .mode("overwrite")
#     .json(f"{ROOT}/code/my_practice/nested_schema/json_dir")
# )
#
# # read df from json file:
# df_json = (
#     spark
#     .read
#     .schema(part_schema)
#     .json(f"{ROOT}/code/my_practice/nested_schema/json_dir/*.json")
# )
#
# print('df_json')
# df_json.show()
# df_json.printSchema()

# sc = df_json.schema
# print(type(sc), sc)

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


def csv_extractor(sep, path):
    reader = spark.read.option("inferSchema", False)
    return reader.csv(path, sep=sep, header=True)


df_csv = csv_extractor(",", f'{ROOT}/code/my_practice/nested_schema/annex_sector_risk_iafa.csv')
# df_csv_vert = csv_extractor("|", f'{ROOT}/code/my_practice/nested_schema/annex_sector_risk_iafa_vert.csv')

df_csv.write.mode('overwrite').csv(path=f'{ROOT}/code/my_practice/nested_schema/annex_sector_risk_iafa_vert', sep='|',
                                   header=True)
df_csv_vert = csv_extractor("|", f'{ROOT}/code/my_practice/nested_schema/annex_sector_risk_iafa_vert/*.csv')
df_csv_dat = csv_extractor("|", f'{ROOT}/code/my_practice/nested_schema/annex_sector_risk_iafa.dat')
df_csv_gz = csv_extractor("|", f'{ROOT}/code/my_practice/nested_schema/annex_sector_risk_iafa.csv.gz')

print(df_csv_vert.schema == df_csv.schema == df_csv_dat.schema)
print(df_csv_gz.schema == df_csv.schema)


print(sorted(df_csv_vert.collect()) == sorted(df_csv.collect()) == sorted(df_csv_dat.collect()))
print(sorted(df_csv_gz.collect()) == sorted(df_csv.collect()) == sorted(df_csv_dat.collect()))


print(df_csv_gz.schema)
df_csv_gz.show()

df_data = df_csv_vert = csv_extractor(",", f'{ROOT}/code/my_practice/nested_schema/data1.csv')

df_data.show()