from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as T
import pyspark.sql.functions as F

# from utils import get_project_root
from my_code.utils import ROOT

print(ROOT)

jars = [
    f"{ROOT}/jars/spark-excel_2.12-0.13.7.jar",
    f"{ROOT}/jars/poi-ooxml-schemas-4.1.2.jar",
    f"{ROOT}/jars/commons-collections4-4.4.jar",
    f"{ROOT}/jars/xmlbeans-3.1.0.jar"
]

# jars_packages = [
#     "com.crealytics:spark-excel_2.12:0.13.7",
#     "org.apache.poi:poi-ooxml-schemas:4.1.2",
#     "org.apache.commons:commons-collections4:4.4",
#     "org.apache.xmlbeans:xmlbeans:3.1.0"
# ]

spark = SparkSession.builder \
    .master("local[*]") \
    .config('spark.jars', ",".join(jars)) \
    .getOrCreate()

# schema = T.StructType([
#     T.StructField('GASS Code', T.StringType()),
#     T.StructField('Description', T.StringType()),
#     T.StructField('AMA my_code', T.StringType()),
#     T.StructField('Fourth', T.StringType()),
#     T.StructField('Fifth', T.StringType()),
# ])


reader = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", True)
)

schema = T.StructType([
    T.StructField('first_col', T.StringType()),
    T.StructField('column2', T.StringType()),
    T.StructField('column3', T.IntegerType())
])


if schema:
    reader = reader.schema(schema)
else:
    reader = reader.option("inferSchema", True)


df = reader.load(f'{ROOT}/source_data/test_excel_file.xlsx').cache()
df.show(truncate=False)
df.printSchema()

for field in df.schema:
    if isinstance(field.dataType, T.IntegerType):
        df = df.withColumn(f'{field.name}_cast_to_doubled', F.col(field.name).cast('double'))
        df = df.withColumn(f'{field.name}_mult_four', F.col(field.name)*4)

df.show(truncate=False)
df.printSchema()
print(df.count())


# def extract(
#     path: str,
#     schema: Optional[T.StructType] = None
# ) -> DataFrame:
#     reader = (
#         spark
#         .read
#         .format("com.crealytics.spark.excel")
#         .option("header", True)
#         .option("inferSchema", False)
#     )
#     if schema:
#         reader = reader.schema(schema)
#     loaded_df = reader.load(path)
#     return loaded_df.na.drop("all")
#
#
# excel_df = extract(f'{ROOT}/data/gl_accounts_iafa_20220128_142516.xlsx')
# excel_df.show(excel_df.count(), truncate=True)
# print(excel_df.count())


#
#
# df_without_schema = spark.read.format("com.crealytics.spark.excel") \
#     .option("header", True) \
#     .option("inferSchema", False) \
#     .load(f'{ROOT}/source_data/test_excel_file.xlsx')
#
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


# .option('dataAddress', "'List_of_accounts_IS'!A1") \


# .load(f'{ROOT}/source_data/GL_Accounts_of_Interest_20210928_142516.xlsx')


# .schema(schema) \


#     .schema(schema)
#     .option("sheetName", sheet)
#     .option("useHeader", header)
#     .option("treatEmptyValuesAsNulls", "true")
#     .option("maxRowsInMemory", 10000)
#     .option("addColorColumns", "False")
#     .load(s"${file}")
# }
