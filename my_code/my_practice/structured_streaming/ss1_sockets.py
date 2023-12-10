#
# start server: nc -lk 9999
#  run job:

# python my_code/my_practice/structured_streaming/ss1_sockets.py localhost 9999
# spark-submit /home/vladyslav_podrazhanskyi/projects/python/learn_spark/my_code/my_practice/structured_streaming/ss1_sockets.py localhost 9999

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

print(type(lines))  # <class 'pyspark.sql.dataframe.DataFrame'>
print(lines.isStreaming)  # <class 'pyspark.sql.dataframe.DataFrame'>

#  Note that the query on streaming lines DataFrame to generate wordCounts is exactly the same
#  as it would be a static DataFrame. However, when this query is started,
#  Spark will continuously check for new data from the socket connection.
#  If there is new data, Spark will run an “incremental” query that combines the previous running counts
#  with the new data to compute updated counts



# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

print(type(words)) # <class 'pyspark.sql.dataframe.DataFrame'>




# Generate running word count
wordCounts = words.groupBy("word").count()

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .start()

print(type(query))  # <class 'pyspark.sql.streaming.StreamingQuery'>

query.awaitTermination()

