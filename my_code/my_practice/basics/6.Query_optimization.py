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

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

sales_df = (
    spark
    .read
    .parquet(f"{ROOT}/my_code/my_practice/basics/sales_data_source/")
)


sales_df.show()

sales_df.printSchema()
"""
root
 |-- order_id: long (nullable = true)
 |-- email: string (nullable = true)
 |-- transaction_timestamp: long (nullable = true)
 |-- total_item_quantity: long (nullable = true)
 |-- purchase_revenue_in_usd: double (nullable = true)
 |-- unique_items: long (nullable = true)
 |-- items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- coupon: string (nullable = true)
 |    |    |-- item_id: string (nullable = true)
 |    |    |-- item_name: string (nullable = true)
 |    |    |-- item_revenue_in_usd: double (nullable = true)
 |    |    |-- price_in_usd: double (nullable = true)
 |    |    |-- quantity: long (nullable = true)

"""

exploded_df = (
    sales_df
    .withColumn("exploded_items", sf.explode("items"))
)

exploded_df.select("items", "exploded_items").show(truncate=False)

exploded_df.printSchema()



# inside column "items" -  list of structs
# each items - nested schemas of columns for separate item
items = [
    {"NEWBED10", "M_STAN_Q", "Standard Queen Mattress", 940.5, 1045.0, 1},
    {"NEWBED10", "P_DOWN_S", "Standard Down Pillow", 107.10000000000001, 119.0, 1}
]

# inside column "exploded_items" -  struct -  nested schema of columns for separate item
#         coupon,     item_id,           item_name, item_revenue_in_usd, price_in_usd, quantity
item = {"NEWBED10", "M_STAN_Q", "Standard Queen Mattress", 940.5,            1045.0,      1}
# access to column in nested schema through dot notation: "item.item_id"
# after select it with dot notation, column have name after dot that is item_id



print(sales_df.count())     # 58
print(exploded_df.count())   # 62  number of rows increased after explode


details_df = (
    sales_df
    .withColumn("items", sf.explode(sf.col("items")))
    .select("email", "items.item_name")                        # access to column in nested schema through dot notation
    .withColumn("details", sf.split("item_name", " "))
)


details_df.printSchema()
"""
root
 |-- email: string (nullable = true)
 |-- item_name: string (nullable = true)
 |-- details: array (nullable = true)
 |    |-- element: string (containsNull = false)
"""
details_df.show(truncate=False)
print(details_df.count())  # 62

"""
String Functions
Here are some of the built-in functions available for manipulating strings.

Method	Description
translate	           Translate any character in the src by a character in replaceString
regexp_replace         Replace all substrings of the specified string value that match regexp with rep
regexp_extract         Extract a specific group matched by a Java regex, from the specified string column
ltrim	               Removes the leading space characters from the specified string column
lower	               Converts a string column to lowercase
split	               Splits str around matches of the given pattern (converts string to list of strings)
"""

# For example: let's imagine that we need to parse our email column.
# We're going to use the split function to split domain and handle.

(
    sales_df
    .select("email")
    .withColumn("email_handle", sf.split("email", "@", 0))
    .show(truncate=False)
)

"""
Collection Functions
Here are some of the built-in functions available for working with arrays.

Method	                                 Description
array_contains	      Returns null if the array is null, true if the array contains value, and false otherwise.
                      inside withColumn -  create boolean column
                      inside filter - filter rows
element_at	          Returns element of array at given index. Array elements are numbered starting with 1.
explode	              Creates a new row for each element in the given array or map column.
collect_set           Returns a set of objects with duplicate elements eliminated.

"""

mattress_df = (
    details_df
    .filter(sf.array_contains(sf.col('details'), "Mattress"))
    .withColumn("size", sf.element_at(sf.col("details"), 2))
)

mattress_df.show(truncate=False)

"""
Aggregate Functions ( used inside agg after groupBy)
Here are some of the built-in aggregate functions available for creating arrays, typically from GroupedData.

Method	                          Description

collect_list	          Returns an array consisting of all values within the group.
collect_set	              Returns an array consisting of all unique values within the group.

"""

mattress_df.printSchema()
"""
root
 |-- email: string (nullable = true)
 |-- item_name: string (nullable = true)
 |-- details: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- size: string (nullable = true)

"""


size_df = (
    mattress_df
    .groupBy("email")
    .agg(sf.collect_set("size").alias("size_options"))
    # .filter(sf.array_size(sf.col("size_options")) > 1)
)

size_df.show()
size_df.printSchema()
"""
root
 |-- email: string (nullable = true)
 |-- collect_set(size): array (nullable = false)
 |    |-- element: string (containsNull = false)
 
 AS in sql after groupBy 
 only groupBy columns 
 and agregations columns are left 
"""

"""
Union and unionByName

Warning The DataFrame 
union method 
resolves columns by position, as in standard SQL. 
You should use it only if the two DataFrames have exactly the same schema, 
including the column order. 

In contrast, the DataFrame 
unionByName method 
resolves columns by name. This is equivalent to UNION ALL in SQL.
 Neither one will remove duplicates!!!!

Below is a check to see if the two dataframes have a matching schema where union would be appropriate

"""

print(mattress_df.schema == size_df.schema)

# to have 2 df with the same schema
# we can select the same col with from 2 df:

union_email = (
    mattress_df
    .select("email")
    .union(size_df.select("email"))
)

print(mattress_df.count())
print(size_df.count())
print(union_email.count())

assert mattress_df.count() + size_df.count() == union_email.count()



