import os
from pyspark import SparkContext, SparkConf
from my_code import ROOT
from datetime import date, datetime
from pyspark.sql import SparkSession, functions as sf

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


data = [
    ('James', 'Smith', 'M', 3000, datetime(2022, 11, 29, 11, 17, 28)),
    ('Anna', 'Rose', 'F', 4100, datetime(2022, 11, 29, 11, 17, 28)),
    ('Robert', 'Williams', 'M', 1999, None),
]

columns = ["firstname", "lastname", "gender", "salary", 'date']
df = spark.createDataFrame(data=data, schema=columns)

df.show()
#
schema = df.schema
df.printSchema()
print(df.count())

# gen with yield
def reformat_gen(partition_data):
    for row in partition_data:
        row_dict = row.asDict()
        print(row_dict)
        # firstname = row.firstname
        # lastname = row.lastname
        # name = firstname + ', ' + lastname
        # gender = row.gender.lower()
        # salary = row.salary
        yield row_dict

# #
# # # # gen with iter without yield
# # # def reformat_gen2(partition_data):
# # #     updated_data = []
# # #     for row in partition_data:
# # #         firstname = row.firstname
# # #         lastname = row.lastname
# # #         name = firstname + ', ' + lastname
# # #         gender = row.gender.lower()
# # #         salary = row.salary
# # #         updated_data.append((name, gender, salary))
# # #     return iter(updated_data)
# #
# #
#
map_part_rdd = df.rdd.mapPartitions(reformat_gen)
df2 = spark.createDataFrame(map_part_rdd, schema)

df2.show(truncate=False)
#
df_par = (
    spark
    .read
    .parquet(f"{ROOT}/my_code/my_practice/RDD/parq_files/")
)

df_par.show()
print(df_par.count())


ts_schema = df_par.schema


def ts_gen(partition_data):
    for row in partition_data:
        row_dict = row.asDict()
        # print(row_dict)
        # firstname = row.firstname
        # lastname = row.lastname
        # name = firstname + ', ' + lastname
        # gender = row.gender.lower()
        # salary = row.salary
        yield row_dict


ts_part_rdd = df_par.rdd.mapPartitions(ts_gen)
ts_df2 = spark.createDataFrame(ts_part_rdd, ts_schema)

ts_df2.show()
print(ts_df2.count())

