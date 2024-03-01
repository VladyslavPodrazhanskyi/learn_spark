import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

# spark reader
df = (
    spark
    .read.format("json")
    .load(f"{ROOT}/source_data/flight-data/json/")
)

df = df.withColumn('double', sf.col("count") * 2)

df.show()
print(df.count())  # 1502

"""
groupBy
Use the DataFrame groupBy method to create a grouped data object.
This grouped data object is called RelationalGroupedDataset in Scala 
and GroupedData in Python.

"""

group_object = df.groupby('DEST_COUNTRY_NAME')
print(group_object)
# GroupedData[grouping expressions:
# [DEST_COUNTRY_NAME], value: [DEST_COUNTRY_NAME: string, ORIGIN_COUNTRY_NAME: string ... 2 more fields], type: GroupBy]

print(type(group_object))   # <class 'pyspark.sql.group.GroupedData'>


'''
Grouped data methods
Various aggregation methods are available on the GroupedData object.

Method	                 Description
agg	           Compute aggregates by specifying a series of aggregate columns
avg	           Compute the mean value for each numeric columns for each group
count	       Count the number of rows for each group
max	           Compute the max value for each numeric columns for each group
mean	       Compute the average value for each numeric columns for each group
min	           Compute the min value for each numeric column for each group
pivot	       Pivots a column of the current DataFrame and performs the specified aggregation
sum	           Compute the sum for each numeric columns for each group
'''

# after groupBy used grouped data methods  sum and avg without agg
df.groupby('DEST_COUNTRY_NAME').sum("count", "double").show()
df.groupby('DEST_COUNTRY_NAME').avg("count", "double").show()

# abandoned_items_df = (
#   abandoned_carts_df
#   .withColumn("items", explode(col("cart")))
#   .groupBy("items").count().alias("count")  # count()
#   # sort("items")
# )


''' 
Aggregate Functions
Here are some of the built-in functions available for aggregation.

Method	                               Description
approx_count_distinct	Returns the approximate number of distinct items in a group
avg	                    Returns the average of the values in a group
collect_list	        Returns a list of objects with duplicates
corr                	Returns the Pearson Correlation Coefficient for two columns
max	                    Compute the max value for each numeric columns for each group
mean	                Compute the average value for each numeric columns for each group
stddev_samp         	Returns the sample standard deviation of the expression in a group
sumDistinct         	Returns the sum of distinct values in the expression
var_pop	                Returns the population variance of the values in a group

Use the grouped data method agg to apply built-in aggregate functions

This allows you to apply other transformations on the resulting columns, such as alias.

'''

# After groupBy used pyspark.sql.functions sum and avg inside agg
# Apply multiple aggregate functions on grouped data

(
    df
    .groupby('DEST_COUNTRY_NAME')
    .agg(
        sf.sum("count").alias('sum_count'),
        sf.avg("count").alias('avg_count'),
        sf.sum("double").alias('sum_double'),
        sf.avg("double").alias('avg_double')
    ).show()
)



