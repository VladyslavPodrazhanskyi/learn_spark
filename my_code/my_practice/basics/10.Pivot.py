"""
https://sparkbyexamples.com/pyspark/pyspark-pivot-and-unpivot-dataframe/

Pivot PySpark DataFrame
PySpark SQL provides pivot() function
to rotate the data from one column into multiple columns.
It is an aggregation where one of the grouping columns values
is transposed into individual columns with distinct data.
To get the total amount exported to each country of each product,
will do group by Product, pivot by Country, and the sum of Amount.

additional link:
https://medium.com/towards-data-engineering/efficient-data-processing-with-pysparks-pivot-and-stack-functions-in-databricks-fa97261ec430

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

data = [
    ("Banana", 1000, "USA"),
    ("Carrots", 1500, "USA"),
    ("Beans", 1600, "USA"),
    ("Orange", 2000, "USA"),
    ("Orange", 2000, "USA"),
    ("Banana", 400, "China"),
    ("Carrots", 1200, "China"),
    ("Beans", 1500, "China"),
    ("Orange", 4000, "China"),
    ("Banana", 2000, "Canada"),
    ("Carrots", 2000, "Canada"),
    ("Beans", 2000, "Mexico")
]

columns = ["Product", "Amount", "Country"]
df = spark.createDataFrame(data=data, schema=columns).orderBy("Product", "Country")
df.printSchema()
print("original df")
df.show(truncate=False)

(
    df
    .groupby("Product")
    .pivot("Country")   # values in col country became separate columns
    .sum("Amount")
    .show()
)

'''
+-------+------+-----+------+----+
|Product|Canada|China|Mexico| USA|
+-------+------+-----+------+----+
| Orange|  NULL| 4000|  NULL|4000|
|  Beans|  NULL| 1500|  2000|1600|
| Banana|  2000|  400|  NULL|1000|
|Carrots|  2000| 1200|  NULL|1500|
+-------+------+-----+------+----+
'''

# pivot is a method of pyspark.sql.group.GroupedData  ( not a dataframe method).

print(type(df))                                                    # <class 'pyspark.sql.dataframe.DataFrame'>
print(type(df.groupby("Product")))                                 # <class 'pyspark.sql.group.GroupedData'>
print(type(df.groupby("Product").pivot("Country")))                # <class 'pyspark.sql.group.GroupedData'>
print(type(df.groupby("Product").pivot("Country").sum("Amount")))  # <class 'pyspark.sql.dataframe.DataFrame'>


# note that pivot is a very expensive operation hence, it is recommended
# to provide column data (if known) as an argument to function as shown below:


countries = ["Canada", "China", "Mexico", "USA", "Ukraine"]
# it's possible to add additional countries and get null
# or choose some of existing countries values not all

(
    df
    .groupby("Product")
    .pivot("Country", countries)
    .sum("Amount")
    .show()
)

# two - phase aggregation.PySpark 2.0
# uses this implementation in order to
# improve the  performance Spark - 13749
# https://issues.apache.org/jira/browse/SPARK-13749


pivotDF = (
    df
    .groupby("Product", "Country")
    .sum("Amount")
    .groupby("Product")
    .pivot("Country")
    .sum("sum(Amount)")
)
print("pivot")
pivotDF.show()
# Unpivot PySpark DataFrame
#
# Unpivot is a reverse operation, we can achieve by rotating column values
# into rows values.
# PySpark SQL doesn’t have unpivot function hence will use the stack() function.
# Below code converts column countries to row.


# todo : analyze unpivot and compare with grouppedDF

# Applying unpivot()
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unPivotDF = (
    pivotDF
    .select("Product", sf.expr(unpivotExpr))
    .where("Total is not null")
)
print("unpivot")
unPivotDF.show(truncate=False)

print("groupped_df")
grouppedDF = (
    df
    .groupBy("Product", "Country")
    .sum("Amount")
)
grouppedDF.show()


# pyspark stack function:
# Separates col1, …, colk into n rows.
# Uses column names col0, col1, etc. by default unless specified otherwise
df = spark.createDataFrame(
    [
        (1, 2, 3),
        (4, 5, 6),
        (7, 8, 9)
    ],
    ["a", "b", "c"]
)

# sf.lit(n) - number or rows to be separated 1 row of data
# for n = 1 we have the original dataframe with renamed columns
stacked1DF = df.select(
    sf.stack(
        sf.lit(1),
        "a", "b", "c"
    )
)
stacked1DF.show()

stacked2DF = df.select(
    sf.stack(
        sf.lit(2),
        "a", "b", "c"
    )
)
stacked2DF.show()
"""
+----+----+
|col0|col1|
+----+----+
|   1|   2|
|   3|NULL|
|   4|   5|
|   6|NULL|
|   7|   8|
|   9|NULL|
+----+----+
"""

stacked3DF = df.select(
    sf.stack(
        sf.lit(3),
        "a", "b", "c"
    )
)
stacked3DF.show()
"""
+----+
|col0|
+----+
|   1|
|   2|
|   3|
|   4|
|   5|
|   6|
|   7|
|   8|
|   9|
+----+
"""

stacked5DF = df.select(
    sf.stack(
        sf.lit(5),
        "a", "b", "c"
    )
)
stacked5DF.show()
"""
+----+
|col0|
+----+
|   1|
|   2|
|   3|
|NULL|
|NULL|
|   4|
|   5|
|   6|
|NULL|
|NULL|
|   7|
|   8|
|   9|
|NULL|
|NULL|
+----+
"""