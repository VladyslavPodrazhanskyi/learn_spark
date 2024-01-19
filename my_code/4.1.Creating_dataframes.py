from my_code import ROOT

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row


spark = SparkSession.builder.master("local[*]").getOrCreate()

# Define the schema for the DataFrame
schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
])

# Create Row objects
rows = [
    Row(name="Alice", age=25),
    Row(name="Bob", age=30),
    Row(name="Charlie", age=35),
]

# Create a DataFrame with Row objects and specified schema
df = spark.createDataFrame(rows, schema)

# Show the DataFrame
df.show()
