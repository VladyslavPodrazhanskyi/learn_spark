from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()

nulls_df = spark.createDataFrame(
    data=[
        (None, None, None, None),
        (None, None, None, None),
        (None, None, None, None)
    ],
    schema="user_id:string, user_first_touch_timestamp:long, email:string, updated:timestamp"
)

dirty_users_df = (
    spark
    .read
    .parquet(f"{ROOT}/my_code/my_practice/basics/users_data_source")
    .withColumn("updated", sf.current_timestamp())
    .union(nulls_df)
)

dirty_users_df.printSchema()
dirty_users_df.show()
print(dirty_users_df.count())  # 987    (pure_count = 983 without nulls)

'''

Inspect Missing Data
Based on the counts above, it looks like there are at least a handful of null values in all of our fields.

NOTE: Null values behave incorrectly in some math functions, including count().

count(col) skips NULL values when counting specific columns or expressions.

count(*) is a special case that counts the total number of rows (including rows that are only NULL values).

We can count null values in a field by filtering for records where that field is null, using either:
count_if(col IS NULL) or count(*) with a filter for where col IS NULL.

Both statements below correctly count records with missing emails.


%sql
SELECT count_if(email IS NULL) FROM users_dirty;
SELECT count(*) FROM users_dirty WHERE email IS NULL;

%python
from pyspark.sql.functions import col
usersDF = spark.read.table("users_dirty")

usersDF.selectExpr("count_if(email IS NULL)")
usersDF.where(col("email").isNull()).count()

'''

print(
    dirty_users_df
    .where(sf.col("email").isNull())
    .count()
)  # 848

'''
Deduplicate Rows

We can use DISTINCT * to remove true duplicate records where entire rows contain the same values.

%sql
SELECT DISTINCT(*) FROM users_dirty

%python
usersDF.distinct().display()

'''

dirty_users_df.distinct().show()

'''
Deduplicate Rows Based on Specific Columns
The code below uses GROUP BY to remove duplicate records based on user_id and user_first_touch_timestamp column values. 
(Recall that these fields are both generated when a given user is first encountered, thus forming unique tuples.)

Here, we are using the aggregate function max as a hack to:

Keep values from the email and updated columns in the result of our group by
Capture non-null emails when multiple records are present

%sql
CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;


SELECT count(*) FROM users_dirty;
-- 986

SELECT count(*) FROM deduped_users;
-- 917


%python
from pyspark.sql.functions import max
dedupedDF = (usersDF
    .where(col("user_id").isNotNull())
    .groupBy("user_id", "user_first_touch_timestamp")
    .agg(max("email").alias("email"), 
         max("updated").alias("updated"))
    )

dedupedDF.count()

'''

dedupedDF = (
    dirty_users_df
    .where(sf.col("user_id").isNotNull())
    .groupBy("user_id", "user_first_touch_timestamp")
    # .count()
    .agg(
        sf.max("email").alias("email"),
        sf.max("updated").alias("updated")
    )
)

dedupedDF.show()
print(dedupedDF.count())  # 917

# Let's confirm that we have the expected count of remaining records
# after deduplicating based on distinct user_id and user_first_touch_timestamp values.

simple_deduplicate_df = (
    dirty_users_df
    .where(sf.col("user_id").isNotNull())
    .dropDuplicates(["user_id", "user_first_touch_timestamp"])
)

print(simple_deduplicate_df.count())  # 917
simple_deduplicate_df.show()

"""
Validate Datasets

Based on our manual review above, 
we've visually confirmed that our counts are as expected.

We can also programmatically perform validation
using simple filters and WHERE clauses.

Validate that the user_id for each row is unique.

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

"""

(
    dedupedDF
    .groupBy("user_id")
    .agg(sf.count("*").alias("count"))  # count()
    .select((sf.max(sf.col("count")) <= 1).alias("no_duplicate_ids"))
    .show()
)

"""
Confirm that each email is associated with at most one user_id.

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

"""

(
    dedupedDF
    .where(sf.col("email").isNotNull())
    .groupBy("email")
    .agg(sf.count("user_id").alias("user_id_count"))
    .select((sf.max(sf.col("user_id_count")) <= 1).alias("at_most_one_id"))
    .show()
)

"""
Date Format and Regex
Now that we've removed null fields and eliminated duplicates, 
we may wish to extract further value out of the data.

The code below:

1) Correctly scales and casts the user_first_touch_timestamp to a valid timestamp
2) Extracts the calendar date and clock time for this timestamp in human readable format
3) Uses regexp_extract to extract the domains from the email column using regex

SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)


explain pattern:  "(?<=@).+", 0

The regular expression used ensures that only the part of the email address 
that comes after the "@" symbol is extracted

"""

dedupedDF.printSchema()
"""
root
 |-- user_id: string (nullable = true)
 |-- user_first_touch_timestamp: long (nullable = true)
 |-- email: string (nullable = true)
 |-- updated: timestamp (nullable = true)
"""
dedupedDF.show()
"""
+-----------------+--------------------------+--------------------+--------------------+
|          user_id|user_first_touch_timestamp|               email|             updated|
+-----------------+--------------------------+--------------------+--------------------+
|UA000000107335605|          1593873851950592|danielcarrillo@sm...|2024-03-02 17:25:...|
|UA000000107338110|          1593874163848994|blackburnjohn@gay...|2024-03-02 17:25:...|
|UA000000107342625|          1593874719224880|  xjoseph@miller.biz|2024-03-02 17:25:...|
|UA000000107345540|          1593875054653990|theresahuber@yaho...|2024-03-02 17:25:...|

"""
(
    dedupedDF
    .withColumn("first_touch", (sf.col("user_first_touch_timestamp") / 1e6).cast(st.TimestampType()))
    .withColumn("first_touch_date", sf.date_format(sf.col("first_touch"), "MMM d, yyyy"))
    .withColumn("first_touch_time", sf.date_format(sf.col("first_touch"), "HH:mm:ss"))
    .withColumn("email_domain", sf.regexp_extract(sf.col("email"), "(?<=@).+", 0))
    .withColumn("email_user_name", sf.regexp_extract(sf.col("email"), "^(.*?)@", 1))
    .show()
)
