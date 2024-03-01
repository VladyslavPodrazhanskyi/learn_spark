"""
Query Optimization
We'll explore query plans and optimizations
for several examples including logical optimizations
and examples with and without predicate pushdown.

Objectives:

1. Logical optimizations
2. Predicate pushdown
3. No predicate pushdown

DataFrame method: explain

"""

"""
Logical Optimization

explain(..) prints the query plans, 
optionally formatted by a given explain mode. 
Compare the following logical plan & physical plan, 
noting how Catalyst handled the multiple filter transformations.


"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

events_df = (
    spark
    .read
    .parquet(f"{ROOT}/my_code/my_practice/basics/events_data_source/")
)

events_df.show()

events_df.printSchema()
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
 |-- items: array (nullable = true)
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
"""
Logical Optimization
explain(..) prints the query plans,
 
optionally formatted by a given explain mode. 

Compare the following logical plan & physical plan, 
noting how Catalyst handled the multiple filter transformations.

"""

limit_events_df = (
    events_df
    .filter(sf.col("event_name") != "reviews")
    .filter(sf.col("event_name") != "checkout")
    .filter(sf.col("event_name") != "register")
    .filter(sf.col("event_name") != "email_coupon")
    .filter(sf.col("event_name") != "cc_info")
    .filter(sf.col("event_name") != "delivery")
    .filter(sf.col("event_name") != "shipping_info")
    .filter(sf.col("event_name") != "press")
)

print(events_df.count())  # 1457472
print(limit_events_df.count())  # 1156071

limit_events_df.explain(extended=False)
"""
== Physical Plan ==
*(1) Filter ((((((((isnotnull(event_name#2) AND NOT (event_name#2 = reviews)) AND NOT (event_name#2 = checkout)) AND NOT (event_name#2 = register)) AND NOT (event_name#2 = email_coupon)) AND NOT (event_name#2 = cc_info)) AND NOT (event_name#2 = delivery)) AND NOT (event_name#2 = shipping_info)) AND NOT (event_name#2 = press))
+- *(1) ColumnarToRow
   +- FileScan parquet [device#0,ecommerce#1,event_name#2,event_previous_timestamp#3L,event_timestamp#4L,geo#5,items#6,traffic_source#7,user_first_touch_timestamp#8L,user_id#9] Batched: true, DataFilters: [isnotnull(event_name#2), NOT (event_name#2 = reviews), NOT (event_name#2 = checkout), NOT (event..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/vladyslav_podrazhanskyi/projects/PERSONAL/python/learn_spar..., PartitionFilters: [], PushedFilters: [IsNotNull(event_name), Not(EqualTo(event_name,reviews)), Not(EqualTo(event_name,checkout)), Not(..., ReadSchema: struct<device:string,ecommerce:struct<purchase_revenue_in_usd:double,total_item_quantity:bigint,u...
"""

limit_events_df.explain(extended=True)

"""
== Physical Plan ==
.....
== Parsed Logical Plan ==
.....
== Analyzed Logical Plan ==
.....
== Optimized Logical Plan ==
.....
== Physical Plan ==
.....

== Physical Plan ==
*(1) Filter ((((((((isnotnull(event_name#2) AND NOT (event_name#2 = reviews)) AND NOT (event_name#2 = checkout)) AND NOT (event_name#2 = register)) AND NOT (event_name#2 = email_coupon)) AND NOT (event_name#2 = cc_info)) AND NOT (event_name#2 = delivery)) AND NOT (event_name#2 = shipping_info)) AND NOT (event_name#2 = press))
+- *(1) ColumnarToRow
   +- FileScan parquet [device#0,ecommerce#1,event_name#2,event_previous_timestamp#3L,event_timestamp#4L,geo#5,items#6,traffic_source#7,user_first_touch_timestamp#8L,user_id#9] Batched: true, DataFilters: [isnotnull(event_name#2), NOT (event_name#2 = reviews), NOT (event_name#2 = checkout), NOT (event..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/vladyslav_podrazhanskyi/projects/PERSONAL/python/learn_spar..., PartitionFilters: [], PushedFilters: [IsNotNull(event_name), Not(EqualTo(event_name,reviews)), Not(EqualTo(event_name,checkout)), Not(..., ReadSchema: struct<device:string,ecommerce:struct<purchase_revenue_in_usd:double,total_item_quantity:bigint,u...


== Parsed Logical Plan ==
'Filter NOT ('event_name = press)
+- Filter NOT (event_name#2 = shipping_info)
   +- Filter NOT (event_name#2 = delivery)
      +- Filter NOT (event_name#2 = cc_info)
         +- Filter NOT (event_name#2 = email_coupon)
            +- Filter NOT (event_name#2 = register)
               +- Filter NOT (event_name#2 = checkout)
                  +- Filter NOT (event_name#2 = reviews)
                     +- Relation [device#0,ecommerce#1,event_name#2,event_previous_timestamp#3L,event_timestamp#4L,geo#5,items#6,traffic_source#7,user_first_touch_timestamp#8L,user_id#9] parquet

== Analyzed Logical Plan ==
device: string, ecommerce: struct<purchase_revenue_in_usd:double,total_item_quantity:bigint,unique_items:bigint>, event_name: string, event_previous_timestamp: bigint, event_timestamp: bigint, geo: struct<city:string,state:string>, items: array<struct<coupon:string,item_id:string,item_name:string,item_revenue_in_usd:double,price_in_usd:double,quantity:bigint>>, traffic_source: string, user_first_touch_timestamp: bigint, user_id: string
Filter NOT (event_name#2 = press)
+- Filter NOT (event_name#2 = shipping_info)
   +- Filter NOT (event_name#2 = delivery)
      +- Filter NOT (event_name#2 = cc_info)
         +- Filter NOT (event_name#2 = email_coupon)
            +- Filter NOT (event_name#2 = register)
               +- Filter NOT (event_name#2 = checkout)
                  +- Filter NOT (event_name#2 = reviews)
                     +- Relation [device#0,ecommerce#1,event_name#2,event_previous_timestamp#3L,event_timestamp#4L,geo#5,items#6,traffic_source#7,user_first_touch_timestamp#8L,user_id#9] parquet

== Optimized Logical Plan ==
Filter (isnotnull(event_name#2) AND (((NOT (event_name#2 = reviews) AND NOT (event_name#2 = checkout)) AND (NOT (event_name#2 = register) AND NOT (event_name#2 = email_coupon))) AND (((NOT (event_name#2 = cc_info) AND NOT (event_name#2 = delivery)) AND NOT (event_name#2 = shipping_info)) AND NOT (event_name#2 = press))))
+- Relation [device#0,ecommerce#1,event_name#2,event_previous_timestamp#3L,event_timestamp#4L,geo#5,items#6,traffic_source#7,user_first_touch_timestamp#8L,user_id#9] parquet

== Physical Plan ==
*(1) Filter ((((((((isnotnull(event_name#2) AND NOT (event_name#2 = reviews)) AND NOT (event_name#2 = checkout)) AND NOT (event_name#2 = register)) AND NOT (event_name#2 = email_coupon)) AND NOT (event_name#2 = cc_info)) AND NOT (event_name#2 = delivery)) AND NOT (event_name#2 = shipping_info)) AND NOT (event_name#2 = press))
+- *(1) ColumnarToRow
   +- FileScan parquet [device#0,ecommerce#1,event_name#2,event_previous_timestamp#3L,event_timestamp#4L,geo#5,items#6,traffic_source#7,user_first_touch_timestamp#8L,user_id#9] Batched: true, DataFilters: [isnotnull(event_name#2), NOT (event_name#2 = reviews), NOT (event_name#2 = checkout), NOT (event..., Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/home/vladyslav_podrazhanskyi/projects/PERSONAL/python/learn_spar..., PartitionFilters: [], PushedFilters: [IsNotNull(event_name), Not(EqualTo(event_name,reviews)), Not(EqualTo(event_name,checkout)), Not(..., ReadSchema: struct<device:string,ecommerce:struct<purchase_revenue_in_usd:double,total_item_quantity:bigint,u...

"""

# Create optimized_limit_events_df created according to the physical plan
optimized_limit_events_df = events_df.filter(
    (sf.col("event_name") != "reviews")
    & (sf.col("event_name") != "checkout")
    & (sf.col("event_name") != "register")
    & (sf.col("event_name") != "email_coupon")
    & (sf.col("event_name") != "cc_info")
    & (sf.col("event_name") != "delivery")
    & (sf.col("event_name") != "shipping_info")
    & (sf.col("event_name") != "press")
)

print(optimized_limit_events_df.count())

optimized_limit_events_df.explain(extended=True)

"""
Of course, we wouldn't write the following code intentionally, 
but in a long, complex query you might not notice 
the duplicate filter conditions. 
Let's see what Catalyst does with this query.
"""


stupid_df = (
    events_df
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
    .filter(sf.col("event_name") != "finalize")
)

print(stupid_df.count())
stupid_df.explain(True)

"""
Caching
By default the data of a DataFrame is present on a Spark cluster only while it is being processed during a query
-- it is not automatically persisted on the cluster afterwards.
 (Spark is a data processing engine, not a data storage system.) 
 You can explicity request Spark to persist a DataFrame on the cluster by invoking its cache method.

If you do cache a DataFrame, you should always explictly evict it from cache 
by invoking unpersist when you no longer need it.

cached_df.unpersist()

Best Practice Caching a DataFrame can be appropriate 
if you are certain 
that you will use the same DataFrame multiple times, as in:

- Exploratory data analysis
- Machine learning model training

!!! Warning Aside from those use cases, 
you should NOT cache DataFrames 
because it is likely that you'll degrade the performance of your application.

Caching consumes cluster resources that could otherwise be used for task execution
Caching can prevent Spark from performing query optimizations, as shown in the next example

"""
"""
Predicate Pushdown
Here is example reading from a JDBC source, where Catalyst determines that predicate pushdown can take place.

%scala
// Ensure that the driver class is loaded
Class.forName("org.postgresql.Driver")

from pyspark.sql.functions import col

jdbc_url = "jdbc:postgresql://server1.training.databricks.com/training"

# Username and Password w/read-only rights
conn_properties = {
    "user" : "training",
    "password" : "training"
}

pp_df = (spark
         .read
         .jdbc(url=jdbc_url,                 # the JDBC URL
               table="training.people_1m",   # the name of the table
               column="id",                  # the name of a column of an integral type that will be used for partitioning
               lowerBound=1,                 # the minimum value of columnName used to decide partition stride
               upperBound=1000000,           # the maximum value of columnName used to decide partition stride
               numPartitions=8,              # the number of partitions/connections
               properties=conn_properties    # the connection properties
              )
         .filter(col("gender") == "M")   # Filter the data by gender
        )

pp_df.explain(True)


Note the lack of a Filter and the presence of a PushedFilters in the Scan. 
The filter operation is pushed to the database and only the matching records are sent to Spark. 
This can greatly reduce the amount of data that Spark needs to ingest.

No Predicate Pushdown
In comparison, caching the data before filtering eliminates the possibility for the predicate push down.

cached_df = (spark
            .read
            .jdbc(url=jdbc_url,
                  table="training.people_1m",
                  column="id",
                  lowerBound=1,
                  upperBound=1000000,
                  numPartitions=8,
                  properties=conn_properties
                 )
            )

cached_df.cache()
filtered_df = cached_df.filter(col("gender") == "M")

filtered_df.explain(True)


In addition to the Scan (the JDBC read) we saw in the previous example, 
here we also see the InMemoryTableScan followed by a Filter in the explain plan.

This means Spark had to read ALL the data from the database and cache it, 
and then scan it in cache to find the records matching the filter condition.

Remember to clean up after ourselves!

cached_df.unpersist()

"""