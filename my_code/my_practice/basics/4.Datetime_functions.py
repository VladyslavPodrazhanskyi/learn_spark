"""
Datetime Functions
Objectives:

1. Cast to timestamp
2. Format datetimes
3. Extract from timestamp
4. Convert to date
5. Manipulate datetimes

Methods:

Column: cast
Built-In Functions: date_format, to_date, date_add, year, month, dayofweek, minute, second

"""

"""
https://sparkbyexamples.com/spark/pyspark-to_timestamp-convert-string-to-timestamp-type/#google_vignette


Syntax: to_timestamp(timestampString:Column)
default format: MM-dd-yyyy HH:mm:ss.SSS

Syntax: to_timestamp(timestampString:Column,format:String)


"""
"""
Built-In Functions: Date Time Functions
Here are a few built-in functions to manipulate dates and times in Spark.

Method                             	Description
add_months	         Returns the date that is numMonths after startDate
current_timestamp    Returns the current timestamp at the start of query evaluation as a timestamp column
date_format	         Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument.
dayofweek	         Extracts the day of the month as an integer from a given date/timestamp/string
from_unixtime	     Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the yyyy-MM-dd HH:mm:ss format
minute	             Extracts the minutes as an integer from a given date/timestamp/string.
unix_timestamp	     Converts time string with given pattern to Unix timestamp (in seconds)


Cast to Timestamp
cast()  - Casts column to a different data type, specified using string representation or DataType.

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.createDataFrame(
    data=[
        ("1", "2019-06-24 12:01:19.000", 1593614089359899),
        ("2", "2019-05-24 12:01:19.000", 1593613901473050),
        ("3", "2023-06-24 12:01:19.000", 1593598782030718)
    ],
    schema=["id", "input_timestamp", "event_timestamp"])

df.printSchema()

# Timestamp String to DateType
df = df.withColumn("timestamp", sf.to_timestamp("input_timestamp"))
df.printSchema()
df.show(truncate=False)

# # Using Cast to convert TimestampType to DateType
df = df.withColumn(
    'timestamp_string',
    sf.to_timestamp('timestamp').cast('string')
).withColumn(
    'date',
    sf.to_date("timestamp")
).withColumn(
    'now',
    sf.current_timestamp()
).withColumn(
    'current_date',
    sf.current_date()
).withColumn(
    "casted_event_timestamp",
    (sf.col("event_timestamp") / 1e6).cast(st.TimestampType())    # cast("timestamp")
)

"""
if not devided on 1e6:
casted_event_timestamp 
+229941-12-04 00:07:57.561024
+229935-12-21 09:20:28.561024
+229456-11-07 20:14:56.561024
"""

df.printSchema()
df.show(truncate=False)


"""
Datetimes
There are several common scenarios for datetime usage in Spark:

CSV/JSON datasources use the pattern string for parsing and formatting datetime content.

Datetime functions related to convert StringType to/from DateType or TimestampType
 e.g. unix_timestamp, date_format, from_unixtime, to_date, to_timestamp, etc.
 
Datetime Patterns for Formatting and Parsing
Spark uses pattern letters for date and timestamp parsing and formatting. A subset of these patterns are shown below.
(https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

Symbol	   Meaning	        Presentation	Examples
G	        era	               text	         AD; Anno Domini
y	        year	           year	         2020; 20
D	     day-of-year     	number(3)	     189
M/L	    month-of-year	      month	         7; 07; Jul; July
d	    day-of-month     	number(3)       	28
Q/q	   quarter-of-year     	number/text	    3; 03; Q3; 3rd quarter
E	   day-of-week           	text	    Tue; Tuesday

Warning Spark's handling of dates and timestamps changed in version 3.0, 
and the patterns used for parsing and formatting these values changed as well. 
For a discussion of these changes, please reference this Databricks blog post.

https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html

"""


"""
Format date
date_format()     -  timestamp to string

Converts a date/timestamp/string 
to a string 
formatted with the given date time pattern.

"""

formatted_df = (
    df.select(
        "id", "event_timestamp", "casted_event_timestamp"
    ).withColumn(
        "date string", 
        sf.date_format("casted_event_timestamp", "MMMM dd, yyyy")
    ).withColumn(
        "time string",
        sf.date_format("casted_event_timestamp", "H:mm:ss.SSSSSS")
    )
)

formatted_df.show()
formatted_df.printSchema()
"""
+---+----------------+----------------------+-------------+---------------+
| id| event_timestamp|casted_event_timestamp|  date string|    time string|
+---+----------------+----------------------+-------------+---------------+
|  1|1593614089359899|  2020-07-01 16:34:...|July 01, 2020|16:34:49.359899|
|  2|1593613901473050|  2020-07-01 16:31:...|July 01, 2020|16:31:41.473050|
|  3|1593598782030718|  2020-07-01 12:19:...|July 01, 2020|12:19:42.030718|
+---+----------------+----------------------+-------------+---------------+

root
 |-- id: string (nullable = true)
 |-- event_timestamp: long (nullable = true)
 |-- casted_event_timestamp: timestamp (nullable = true)
 |-- date string: string (nullable = true)
 |-- time string: string (nullable = true)
"""

"""
Extract datetime attribute from timestamp
Similar methods: 
year, month, dayofweek, minute, second, etc.

Convert to Date
to_date
Converts the column into DateType by casting rules to DateType.

"""

dt_attribute_df = df.select(
    "id",
    "event_timestamp",
    "casted_event_timestamp"
).withColumn(
    "year",
    sf.year(sf.col("casted_event_timestamp"))
).withColumn(
    "month",
    sf.month(sf.col("casted_event_timestamp"))
).withColumn(
    "dayofweek",
    sf.dayofweek(sf.col("casted_event_timestamp"))
).withColumn(
    "minute",
    sf.minute(sf.col("casted_event_timestamp"))
).withColumn(
    "second",
    sf.second(sf.col("casted_event_timestamp"))
).withColumn(
    "date",
    sf.to_date(sf.col("casted_event_timestamp"))
).withColumn(
    "plus_two_days",
    sf.dateadd(sf.col("casted_event_timestamp"), 2)
)

dt_attribute_df.show()
dt_attribute_df.printSchema()

"""
+---+----------------+----------------------+----+-----+---------+------+------+----------+-------------+
| id| event_timestamp|casted_event_timestamp|year|month|dayofweek|minute|second|      date|plus_two_days|
+---+----------------+----------------------+----+-----+---------+------+------+----------+-------------+
|  1|1593614089359899|  2020-07-01 16:34:...|2020|    7|        4|    34|    49|2020-07-01|   2020-07-03|
|  2|1593613901473050|  2020-07-01 16:31:...|2020|    7|        4|    31|    41|2020-07-01|   2020-07-03|
|  3|1593598782030718|  2020-07-01 12:19:...|2020|    7|        4|    19|    42|2020-07-01|   2020-07-03|
+---+----------------+----------------------+----+-----+---------+------+------+----------+-------------+


root
 |-- id: string (nullable = true)
 |-- event_timestamp: long (nullable = true)
 |-- casted_event_timestamp: timestamp (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- dayofweek: integer (nullable = true)
 |-- minute: integer (nullable = true)
 |-- second: integer (nullable = true)
 |-- date: date (nullable = true)
 |-- plus_two_days: date (nullable = true)
"""