""""
In pyspark there are:

1) dataframe methods:

1.1. Transformations:

- withColumn
- withColumnRenamed
- filter
- select
- sort
- join
- selectExpr
- groupBY ( for aggregations)


1.2. Actions:

- printSchema()
- count()
- show()
- take()
- write()
- collect()

===========================================================================

2) Grouped data methods:

agg	           Compute aggregates by specifying a series of aggregate columns
avg	           Compute the mean value for each numeric columns for each group
count	       Count the number of rows for each group
max	           Compute the max value for each numeric columns for each group
mean	       Compute the average value for each numeric columns for each group
min	           Compute the min value for each numeric column for each group
pivot	       Pivots a column of the current DataFrame and performs the specified aggregation
sum	           Compute the sum for each numeric columns for each group

==============================================================

3) pyspark.sql.functions(for column expressions):

Built-In Functions
In addition to DataFrame and Column transformation methods,
there are a ton of helpful functions in Spark's built-in SQL functions module.

In Scala, this is org.apache.spark.sql.functions, and
pyspark.sql.functions in Python.
Functions from this module must be imported into your code.


3.1.Aggregate Functions
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

=========================================================

3.2. Other pyspark.sql.functions (inside select):

- col  ( use for column expressions  ( inside dataframe methods) ).
- monotonically_increasing_id

===================================================

3.3. Math Functions
Here are some of the built-in functions for math operations.

Method	               Description
ceil	       Computes the ceiling of the given column.
cos	           Computes the cosine of the given value.
log	           Computes the natural logarithm of the given value.
round	       Returns the value of the column e rounded to 0 decimal places with HALF_UP round mode.
sqrt	       Computes the square root of the specified float value.

"""

import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()


# spark.sparkContext.setLogLevel("INFO")

# spark catalog
# catalog = spark.catalog
# pprint(catalog.__sizeof__())
# pprint(catalog.listDatabases())
# pprint(catalog.listTables())

# UI: http://127.0.0.1:4040/jobs/

# spark reader
df = spark.read.format("json").load(f"{ROOT}/source_data/flight-data/json/")

# df.withColumnRenamed()


df.show()
print(df.count())  # 1502


df.printSchema()
"""
root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- renamed_ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: long (nullable = true)
"""

print(df.schema)

"""
Column Expressions
A Column is a logical construction that will be computed based on the data in a DataFrame using an expression
Construct a new Column based on existing columns in a DataFrame
"""
print(df.DEST_COUNTRY_NAME)
print(df["DEST_COUNTRY_NAME"])

# not connected with df and can be of any name
print(sf.col("device"))
print(type(sf.col("device")))
# Column<'device'>
# <class 'pyspark.sql.column.Column'>

# %scala
# $"device"

'''
Column Operators and Methods
Method	        Description
*, + , <, >=	Math and comparison operators
==, !=	        Equality and inequality tests (Scala operators are === and =!=)
alias       	Gives the column an alias
cast, astype	Casts the column to a different data type
isNull, isNotNull, isNan	Is null, is not null, is NaN
asc, desc	   Returns a sort expression based on ascending/descending order of the column
'''

# Create complex expressions with existing columns, operators, and methods.

sf.col("ecommerce.purchase_revenue_in_usd") + sf.col("ecommerce.total_item_quantity")
sf.col("event_timestamp").desc()
(sf.col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# Here's an example of using these column expressions in the context of a DataFrame

rev_df = (
    df
    .filter(sf.col("DEST_COUNTRY_NAME").isNotNull())
    .withColumn("double_count", (sf.col('count') * 2).cast('int'))
    .where(sf.col("double_count").isNotNull())
)

rev_df.show()
rev_df.printSchema()

'''
DataFrame            Transformation Methods
Method	             Description
select	             Returns a new DataFrame by computing given expression for each element
drop	             Returns a new DataFrame with a column dropped
withColumnRenamed	 Returns a new DataFrame with a column renamed
withColumn	         Returns a new DataFrame by adding a column or replacing the existing column that has the same name
filter, where	     Filters rows using the given condition
sort, orderBy	     Returns a new DataFrame sorted by the given expressions
dropDuplicates, distinct	Returns a new DataFrame with duplicate rows removed
limit	                    Returns a new DataFrame by taking the first n rows
groupBy	                    Groups the DataFrame using the specified columns, so we can run aggregation on them
'''

"""
Subset columns
Use DataFrame transformations to subset columns

select()
Selects a list of columns or column based expressions
"""

(
    rev_df
    .withColumn("id", sf.monotonically_increasing_id())
    .select(
        'id',
        'count',
        sf.col('double_count').alias('double')
    )
    .show()
)

"""
selectExpr()
Selects a list of SQL expressions
"""

df.selectExpr(
    "DEST_COUNTRY_NAME",
    "ORIGIN_COUNTRY_NAME",
    "ORIGIN_COUNTRY_NAME in ('Singapore', 'India') as from_IND_SING"
).show()

df.selectExpr(
    "*",
    "ORIGIN_COUNTRY_NAME in ('Singapore', 'India') as from_IND_SING"
).show()

df.withColumnRenamed("ORIGIN_COUNTRY_NAME", "renamed_ORIGIN_COUNTRY_NAME").show()

df.filter("count > 200").show()
df.filter(sf.col("count") > 500).show()

"""
sort()
Returns a new DataFrame sorted by the given columns or expressions.
Alias: orderBy
"""
(
    df.filter(
        (sf.col("DEST_COUNTRY_NAME") != "United States")
        & (sf.col("count") > 5)
        & (sf.col("count") < 100)
    ).orderBy(sf.col('count').desc())      # orderBy('count', ascending=False)
    .distinct()
    .limit(19)
    .show()
)

df.withColumn(
    'sqrt_count', sf.sqrt('count')
).withColumn(
'sin_count', sf.sin(sf.col('count'))
).show()

# time.sleep(600)
# spark.stop()


# df_with_five = df.withColumn("five", sf.lit(5.0))
# print(set(df_with_five.schema) - set(df.schema))
# print(set(df.schema) - set(df_with_five.schema))
#
# print(set(df.schema) -  set())


#
# print(df.select('count').rdd)  # MapPartitionsRDD[27] at javaToPython at NativeMethodAccessorImpl.java:0
# print(df.select('count').rdd.max())  # Row(count=370002)
# print(df.select('count').rdd.max()[0])
#
# print(df.select(sf.max('count').alias('max')).collect()[0]['max'])




#
# df.select("ORIGIN_COUNTRY_NAME", "count").show(5)
# df.selectExpr("ORIGIN_COUNTRY_NAME", "count").show(5)
#
# df.select(
#     "DEST_COUNTRY_NAME",
#     sf.expr("DEST_COUNTRY_NAME"),
#     sf.col("DEST_COUNTRY_NAME")
# ).show(5)

# advantage of F.expr in comparison with col
# df.select(F.expr("DEST_COUNTRY_NAME AS destination")).show(2)
# df.select(F.col("DEST_COUNTRY_NAME").alias("destination")).show(2)
#
# # selectExpr !!!
# """
# Because select followed by a series of expr is such a common pattern, Spark has a shorthand
# for doing this efficiently: selectExpr. This is probably the most convenient interface for
# everyday use
# This opens up the true power of Spark. We can treat selectExpr as a simple way to build up
# complex expressions that create new DataFrames.
# """
# df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(5)
#
# df.selectExpr(
#     "*",
#     "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry") \
#     .show(100)
#
#
# # aggregations over the entire DataFrame
#
# df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
#
#
#
# print("sqlWay")
#
# # temporary view for query with SQL
# df.createOrReplaceTempView("dfTable")
#
# sqlWayWithinCountry = spark.sql(
#     """
#     SELECT *,
#     (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry
#     FROM dfTable
#     """
# ).filter(sf.col('withinCountry')).show(12)
#
# pprint(catalog.listTables()) # [Table(name='dftable', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]


# sqlWay = spark.sql("""
# SELECT
# DEST_COUNTRY_NAME, Sum(count) as sum
# FROM dfTable
# GROUP BY DEST_COUNTRY_NAME
# ORDER by sum DESC""").show(10)


