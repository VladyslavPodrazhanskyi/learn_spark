'''
Reader & Writer
Objectives:

Read from CSV files
Read from JSON files


Write DataFrame to files
Write DataFrame to tables
Write DataFrame to a Delta table

Methods
DataFrameReader: csv, json, option, schema
DataFrameWriter: mode, option, parquet, format, saveAsTable

StructType: toDDL
Spark Types

Types: ArrayType, DoubleType, IntegerType, LongType, StringType, StructType, StructField
'''

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

'''
DataFrameReader
Interface used to load a DataFrame from external storage systems

spark.read.parquet("path/to/files")

DataFrameReader is accessible through the SparkSession attribute read. T
his class includes methods to load DataFrames from different external storage systems.
'''

'''
Read from CSV files

Read from CSV with the DataFrameReader's csv method and the following options:

Tab separator, 
use first line as header, 
infer schema

'''

csv_df = (
    spark
    .read
    .option("sep", ",")
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{ROOT}/source_data/flight-data/csv/")
)
csv_df.show()

# alternative syntax for setting reader options
(
    spark
    .read
    .csv(
        f"{ROOT}/source_data/flight-data/csv/",
        sep=',',
        header=True,
        inferSchema=True
    ).show()
)

csv_df.printSchema()
'''
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: decimal(10,0) (nullable = true)
'''


print(csv_df.schema)

user_defined_schema = st.StructType([
    st.StructField('DEST_COUNTRY', st.StringType(), True),
    st.StructField('ORIGIN_COUNTRY', st.StringType(), True),
    st.StructField('FLIGHT_COUNT', st.DecimalType(), True)                 # change from IntegerType
])

# Read from CSV using this user-defined schema instead of inferring the schema

csv_df_with_schema = (
    spark
    .read
    .option("sep", ",")
    .option("header", True)
    .schema(user_defined_schema)
    .csv(f"{ROOT}/source_data/flight-data/csv/")
)

csv_df_with_schema.printSchema()

csv_df_with_schema.show()


#  Alternatively, define the schema using data definition language (DDL) syntax.

ddl_schema = "dc string, oc string, flight_count long"

(
    spark
    .read
    .option("sep", ",")
    .option("header", True)
    .schema(ddl_schema)
    .csv(f"{ROOT}/source_data/flight-data/csv/")
    .printSchema()
)

'''
Read from JSON files
Read from JSON with DataFrameReader's 
json method and the infer schema option

'''

json_df = (
    spark
    .read
    .format("json")
    .load(f"{ROOT}/source_data/flight-data/json/")
)

json_df.show()

(
    spark
    .read
    .option("inferSchema", True)
    .json(f"{ROOT}/source_data/flight-data/json/")
    .show()
)

# Read data faster by creating a StructType with the schema names and data types


json_df.printSchema()


# if col name is changed in schema then new col will have nulls
json_schema = st.StructType([
    st.StructField('DEST_COUNTRY_NAME', st.StringType(), True),
    st.StructField('ORIGIN_COUNTRY_NAME', st.StringType(), True),
    st.StructField('count', st.LongType(), True)                 # change from IntegerType
])

(
    spark
    .read
    .schema(json_schema)
    .json(f"{ROOT}/source_data/flight-data/json/")
    .show()
)

"""
You can use the StructType Scala method toDDL to have a DDL-formatted string created for you.
This is convenient when you need to get the DDL-formated string for ingesting CSV and JSON 
but you don't want to hand craft it or the StructType variant of the schema.
However, this functionality is not available in Python but the power of the notebooks allows us to use both languages.

# Step 1 - use this trick to transfer a value (the dataset path) 
between Python and Scala using the shared spark-config
spark.conf.set("com.whatever.your_scope.events_path", DA.paths.events_json)

In a Python notebook like this one, create a Scala cell to injest the data and produce the DDL formatted schema

%scala
// Step 2 - pull the value from the config (or copy & paste it)
val eventsJsonPath = spark.conf.get("com.whatever.your_scope.events_path")

// Step 3 - Read in the JSON, but let it infer the schema
val eventsSchema = spark.read
                        .option("inferSchema", true)
                        .json(eventsJsonPath)
                        .schema.toDDL

// Step 4 - print the schema, select it, and copy it.
println("="*80)
println(eventsSchema)
println("="*80)

# Step 5 - paste the schema from above and assign it to a variable as seen here
events_schema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

# Step 6 - Read in the JSON data using our new DDL formatted string
events_df = (spark.read
                 .schema(events_schema)
                 .json(DA.paths.events_json))

display(events_df)

This is a great "trick" for producing a schema for a net-new dataset and for accelerating development.

When you are done (e.g. for Step #7), make sure to delete your temporary code.

 WARNING: Do not use this trick in production
the inference of a schema can be REALLY slow as it
forces a full read of the source dataset to infer the schema

"""

"""
DataFrameWriter
Interface used to write a DataFrame to external storage systems

(df
  .write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet(output_dir)
)

DataFrameWriter is accessible through the SparkSession attribute write. 
This class includes methods to write DataFrames to different external storage systems.

"""


"""
Write DataFrames to files
Write users_df to parquet with DataFrameWriter's parquet method 
and the following configurations:

Snappy compression, overwrite mode

Snappy is one of the compression codecs available in Spark 
that allows for efficient compression and decompression of data. 
It is known for its fast compression and decompression speeds, 
making it suitable for scenarios 
where low-latency compression and decompression are essential.
"""

output_dir = f"{ROOT}/my_code/my_practice/basics/write_output_dir/csv_df_to_parquet"

csv_df_with_schema.printSchema()

(
    csv_df_with_schema
    .write
    .option("compression", "snappy")
    .mode("overwrite")
    .parquet(output_dir)
)

# alternative syntax:
# (
#     csv_df_with_schema
#     .write
#     .parquet(output_dir, compression="snappy", mode="overwrite")
# )

"""
Write DataFrames to tables
Write events_df to a table using the DataFrameWriter method saveAsTable
Note This creates a GLOBAL TABLE, 
unlike the local view created by the DataFrame method createOrReplaceTempView
"""

print(spark.catalog.listCatalogs())
print(spark.catalog.listDatabases())
print(spark.catalog.listTables())

# (
#     csv_df_with_schema
#     .write
#     .mode("overwrite")
#     .saveAsTable("flights_from_csv")
# )


'''
Write Results to a Delta Table
Write events_df with the DataFrameWriter's save method 
and the following configurations: Delta format & overwrite mode.

events_output_path = DA.paths.working_dir + "/delta/events"

(
    events_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(events_output_path)
)
'''
