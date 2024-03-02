'''

To create a table against an external source in PySpark,
you can wrap this SQL code with the spark.sql() function.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS sales_csv1
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "{DA.paths.sales_csv}"
""")

CREATE TABLE IF NOT EXISTS events_json
  (key BINARY, offset LONG, partition INTEGER, timestamp LONG, topic STRING, value BINARY)
USING JSON
LOCATION "${DA.paths.kafka_events}"
'''

'''
All the metadata and options passed during table declaration
 will be persisted to the metastore, ensuring that data in the location 
 will always be read with these options.

NOTE: When working with CSVs as a data source, 
it's important to ensure that column order does not change 
if additional data files will be added to the source directory. 
Because the data format does not have strong schema enforcement, 
Spark will load columns and apply column names and data types 
in the order specified during table declaration.

Running DESCRIBE EXTENDED 
on a table will show all of the metadata associated with the table definition.

%sql
DESCRIBE EXTENDED sales_csv
'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()


# To create a table against an external source in PySpark,
# you can wrap this SQL code with the spark.sql() function.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS flight_data_sql
  (DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INTEGER)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ","
)
LOCATION "{ROOT}/source_data/flight-data/csv/"
""")

df = spark.sql("SELECT * FROM flight_data_sql")
df.printSchema()
df.show()
print(df.count())  # 1502


'''
Note that no data has moved during table declaration.

Similar to when we directly queried our files and created a view, 
we are still just pointing to files stored in an external location.

All the metadata and options passed during table declaration 
will be persisted to the metastore, ensuring that data in the location 
will always be read with these options.

NOTE: When working with CSVs as a data source, 
it's important to ensure that column order does not change 
if additional data files will be added to the source directory. 
Because the data format does not have strong schema enforcement, 
Spark will load columns and apply column names and data types
 in the order specified during table declaration.

Running DESCRIBE EXTENDED on a table will show all of the metadata 
associated with the table definition.
'''

print(spark.sql("DESCRIBE EXTENDED flight_data_sql"))  # DataFrame[col_name: string, data_type: string, comment: string]
spark.sql("DESCRIBE EXTENDED flight_data_sql").show(truncate=False)

'''
+----------------------------+-----------------------------------------------------------------------------------------------------+-------+
|col_name                    |data_type                                                                                            |comment|
+----------------------------+-----------------------------------------------------------------------------------------------------+-------+
|DEST_COUNTRY_NAME           |string                                                                                               |NULL   |
|ORIGIN_COUNTRY_NAME         |string                                                                                               |NULL   |
|count                       |int                                                                                                  |NULL   |
|                            |                                                                                                     |       |
|# Detailed Table Information|                                                                                                     |       |
|Catalog                     |spark_catalog                                                                                        |       |
|Database                    |default                                                                                              |       |
|Table                       |flight_data_sql                                                                                      |       |
|Created Time                |Sat Mar 02 13:42:24 CET 2024                                                                         |       |
|Last Access                 |UNKNOWN                                                                                              |       |
|Created By                  |Spark 3.5.0                                                                                          |       |
|Type                        |EXTERNAL                                                                                             |       |
|Provider                    |CSV                                                                                                  |       |
|Location                    |file:///home/vladyslav_podrazhanskyi/projects/PERSONAL/python/learn_spark/source_data/flight-data/csv|       |
|Storage Properties          |[header=true, delimiter=,]                                                                           |       |
+----------------------------+-----------------------------------------------------------------------------------------------------+-------+
'''

"""
Limits of Tables with External Data Sources
If you've taken other courses on Databricks or reviewed any of our company literature,
 you may have heard about Delta Lake and the Lakehouse. 
 Note that whenever we're defining tables or queries against external data sources, 
 we cannot expect the performance guarantees associated with Delta Lake and Lakehouse.

For example: while Delta Lake tables will guarantee 
that you always query the most recent version of your source data, 
tables registered against other data sources may represent older cached versions.

At the time we previously queried this data source, 
Spark automatically cached the underlying data in local storage. 
This ensures that on subsequent queries, 
Spark will provide the optimal performance by just querying this local cache.
Our external data source is not configured to tell Spark that it should refresh this data.

We can manually refresh the cache of our data by running the REFRESH TABLE command.
"""

spark.sql("REFRESH table flight_data_sql")

'''
Note that refreshing our table will invalidate our cache, 
meaning that we'll need to rescan our original data source 
and pull all data back into memory.

For very large datasets, this may take a significant amount of time.

'''


'''
Note that some SQL systems such as data warehouses will have custom drivers. 
Spark will interact with various external databases differently, 
but the two basic approaches can be summarized as either:

1) Moving the entire source table(s) to Databricks and then executing logic on the currently active cluster
2) Pushing down the query to the external SQL database and only transferring the results back to Databricks

In either case, working with very large datasets in external SQL databases 
can incur significant overhead because of either:

Network transfer latency associated with moving all data over the public internet
Execution of query logic in source systems not optimized for big data queries

'''

