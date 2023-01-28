'''

'''
import string
from pprint import pprint
import random

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F
from schemas import full_schema, part_schema

from code import ROOT


def random_string():
    letters = string.ascii_letters
    length = random.randint(4, 6)
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str




def random_url():
    return f"https://www.{random_string()}.com"


randomStringUDF = F.udf(lambda col: random_string())
randomEmailUDF = F.udf(lambda col: f"{random_string()}@alcon.net")




def sorted_df(df):
    return (
        df
        .select(df.columns)
        .orderBy(df.columns)
        .collect()
    )


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

# read df from parquet file:
df_parq = (
    spark
    .read
    # .schema(full_schema)
    .parquet(f"{ROOT}/code/my_practice/nested_schema/parquet/20220203_084812_00030_zins9_a5133584-1277-40f0-be5a-3f24599f519a")
)

df_parq.show()


# write json file
(
    df_parq
    .write
    .option("multiline", True)
    .mode("overwrite")
    .json(f"{ROOT}/code/my_practice/nested_schema/json_iafa-333/")
)



#
# transform_df = (
#     df_parq
#     .withColumn("user_login_id", randomEmailUDF(F.col("user_login_id")))
#     .withColumn(
#         "report_owner",
#         F.col("report_owner")
#         .withField("firstname", F.lit(random_string()))
#         .withField("lastname", F.lit(random_string()))
#         .withField("middleinitial", F.lit(random_string()))
#     )
# )

# print("transform_df")
# transform_df.show()
# print(transform_df.count())
#
# df_parq.show(truncate=False)
# print(df_parq.count())
# print(df_parq.schema)

# write json file
# (
#     transform_df
#     .write
#     .option("multiline", True)
#     .mode("overwrite")
#     .json(f"{ROOT}/code/my_practice/nested_schema/json_dir/")
# )

# read df from json file:
# df_json = (
#     spark
#     .read
#     .option("multiline", True)
#     .schema(full_schema)
#     .json(f"{ROOT}/code/my_practice/nested_schema/json_dir/*.json")  # timestampFormat
# )
#
# print('df_json')
# df_json.show(truncate=True)
# print(df_json.count())
# # print(df_json.schema == df_parq.schema == full_schema)
# print(df_json.schema == full_schema)
# # print(sorted_df(df_json) == sorted_df(df_parq))
#
#
# for i in range(30):
#     print(random_string())