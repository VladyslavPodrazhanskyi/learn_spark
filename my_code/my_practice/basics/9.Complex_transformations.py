from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

"""
Complex Transformations
Querying tabular data stored in the data lakehouse with Spark SQL is easy, efficient, and fast.

This gets more complicated as the data structure becomes less regular,
when many tables need to be used in a single query,
or when the shape of data needs to be changed dramatically.

This notebook introduces a number of functions present in Spark SQL
to help engineers complete even the most complicated transformations.

Learning Objectives
By the end of this lesson, you should be able to:

1) Use . and : syntax to query nested data
2) Parse JSON strings into structs
3) Flatten and unpack arrays and structs
4) Combine datasets using joins
5) Reshape data using pivot tables


"""

"""
Preparing raw data:

Data Overview
The events_raw table was registered against data representing a Kafka payload. 
In most cases, Kafka data will be binary-encoded JSON values.

CREATE OR REPLACE TEMP VIEW events_raw AS 
SELECT * FROM events_raw;

SELECT * FROM events_raw;


key                         offset      partition     timestamp     topic 
VUEwMDAwMDAxMDczODQyMDg=	219254258	    0	    1593880822530	clickstream

Value
eyJkZXZpY2UiOiJtYWNPUyIsImVjb21tZXJjZSI6e30sImV2ZW50X25hbWUiOiJjaGVja291dCIsImV2ZW50X3ByZXZpb3Vz
X3RpbWVzdGFtcCI6MTU5Mzg4MDgwMTAyNzc5NywiZXZlbnRfdGltZXN0YW1wIjoxNTkzODgwODI= (truncated)



Let's cast the key and value as strings to view these values in a human-readable format.

CREATE OR REPLACE TEMP VIEW events_strings AS 
SELECT string(key), string(value) FROM events_raw;

SELECT * FROM events_strings

%python
from pyspark.sql.functions import col

events_stringsDF = (spark
    .table("events_raw")
    .select(col("key").cast("string"),           # binary encoded values cast to readable string
            col("value").cast("string"))                      
    )
display(events_stringsDF)



key	                                      value
UA000000107384208	{"device":"macOS","ecommerce":{},"event_name":"checkout",
                     "event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,
                     "geo":{"city":"Traverse City","state":"MI"},
                     "items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,
                     "price_in_usd":595.0,"quantity":1}],
                     "traffic_source":"google","user_first_touch_timestamp":1593879413256859,
                     "user_id":"UA000000107384208"}

"""

"""
Manipulate Complex Types

Work with Nested Data
The code cell below queries the converted strings to view an example JSON object
 without null fields (we'll need this for the next section).

NOTE: Spark SQL has built-in functionality to directly interact 
with nested data stored as JSON strings or struct types.

Use : syntax in queries to access subfields in JSON strings   ( : notation)
Use . syntax in queries to access subfields in struct types   ( dot notation)

%sql
SELECT * FROM events_strings WHERE value:event_name = "finalize" ORDER BY key LIMIT 1

%python
display(events_stringsDF
    .where("value:event_name = 'finalize'")
    .orderBy("key")
    .limit(1)
)


key	                                                                                    value
UA000000106459577	{"device":"Linux",
                     "ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},
                     "event_name":"finalize","event_previous_timestamp":1593879787820475,
                     "event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},
                     "items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress",
                     "item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10",
                     "item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,
                     "price_in_usd":119.0,"quantity":1}],"traffic_source":"email",
                     "user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}

"""

"""
Let's use the JSON string example above to derive the schema, then parse the entire JSON column into struct types.

schema_of_json() returns the schema derived from an example JSON string.
from_json() parses a column containing a JSON string into a struct type using the specified schema.
After we unpack the JSON string to a struct type, let's unpack and flatten all struct fields into columns.

* unpacking can be used to flattens structs; col_name.* pulls out the subfields of col_name into their own columns.

SELECT schema_of_json({"device":"Linux",
                     "ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},
                     "event_name":"finalize","event_previous_timestamp":1593879787820475,
                     "event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},
                     "items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress",
                     "item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10",
                     "item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,
                     "price_in_usd":119.0,"quantity":1}],"traffic_source":"email",
                     "user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"})
                     
                     
STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, 
unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, 
geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, 
item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, 
user_first_touch_timestamp: BIGINT, user_id: STRING>


CREATE OR REPLACE TEMP VIEW parsed_events 
AS SELECT json.* FROM (
SELECT from_json(
    value, 
    'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, : STRING>'.....
) AS json 
FROM events_strings);

SELECT * FROM parsed_events


json_sting = '''
{"device":"Linux",
 "ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},
 "event_name":"finalize","event_previous_timestamp":1593879787820475,
 "event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},
 "items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress",
 "item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10",
 "item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,
 "price_in_usd":119.0,"quantity":1}],"traffic_source":"email",
 "user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}
'''

parsed_eventsDF = (events_stringsDF
    .select(from_json("value", schema_of_json(json_string)).alias("json"))
    .select("json.*")
)

display(parsed_eventsDF)
parsed_eventsDF.printSchema()


"""

events_hist_df = (
    spark
    .read
    .parquet(f"{ROOT}/my_code/my_practice/basics/events_hist")
)

events_hist_df.printSchema()
"""
root
 |-- device: string (nullable = true)
 |-- ecommerce: struct (nullable = true)
 |    |-- purchase_revenue_in_usd: double (nullable = true)
 |    |-- total_item_quantity: long (nullable = true)
 |    |-- unique_items: long (nullable = true)
 |-- event_name: string (nullable = true)
 |-- event_previous_timestamp: long (nullable = true)
 |-- event_timestamp: long (nullable = true)
 |-- geo: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- state: string (nullable = true)
 |-- items: array_practice (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- coupon: string (nullable = true)
 |    |    |-- item_id: string (nullable = true)
 |    |    |-- item_name: string (nullable = true)
 |    |    |-- item_revenue_in_usd: double (nullable = true)
 |    |    |-- price_in_usd: double (nullable = true)
 |    |    |-- quantity: long (nullable = true)
 |-- traffic_source: string (nullable = true)
 |-- user_first_touch_timestamp: long (nullable = true)
 |-- user_id: string (nullable = true)
"""

events_hist_df.show()
print("events_hist_count: ", events_hist_df.count())  # 485696

sales_hist_df = (
    spark
    .read
    .parquet(f"{ROOT}/my_code/my_practice/basics/sales_hist")
)

sales_hist_df.printSchema()
"""
root
 |-- order_id: long (nullable = true)
 |-- email: string (nullable = true)
 |-- transaction_timestamp: long (nullable = true)
 |-- total_item_quantity: long (nullable = true)
 |-- purchase_revenue_in_usd: double (nullable = true)
 |-- unique_items: long (nullable = true)
 |-- items: array_practice (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- coupon: string (nullable = true)
 |    |    |-- item_id: string (nullable = true)
 |    |    |-- item_name: string (nullable = true)
 |    |    |-- item_revenue_in_usd: double (nullable = true)
 |    |    |-- price_in_usd: double (nullable = true)
 |    |    |-- quantity: long (nullable = true)
"""

sales_hist_df.show()
print(sales_hist_df.count())  # 10510

'''
Manipulate Arrays
Spark SQL has a number of functions for manipulating array_practice data, 
including the following:

explode()   - separates the elements of an array_practice into multiple rows; this creates a new row for each element.
size()       -provides a count for the number of elements in an array_practice for each row.


The code below explodes the items field (an array_practice of structs) 
into multiple rows and shows events containing arrays with 3 or more items.

'''

id_events_hist_df = (
    events_hist_df
    .withColumn("id", sf.monotonically_increasing_id())
)

id_events_hist_df.orderBy(sf.col("id")).show()

#
# exploded_df = (
#     events_hist_df
#     .withColumn("items", sf.col("items"))
#     .withColumn("item", sf.explode("items"))  # # .where(sf.size("items") > 2)
# )
#
# exploded_df.show()
#
# print("events_hist_df_count", events_hist_df.count())  # 485 696
# print("exploded_count:", exploded_df.count())
# exploded_count:
# without where 136 290
# with where 2541
