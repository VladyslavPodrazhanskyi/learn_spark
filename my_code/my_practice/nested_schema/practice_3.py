'''

'''

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

# read df from json file:
df_json = (
    spark
    .read
    # .option("multiline", True)
    .schema(full_schema)
    .json(f"{ROOT}/code/my_practice/nested_schema/test_data.json")
)

print('df_json')
df_json.show(truncate=False)
df_json.printSchema()

df = df_json.limit(2)

count = df_json.count()
count = df.count()

print("count = {count}".format(count=count))


