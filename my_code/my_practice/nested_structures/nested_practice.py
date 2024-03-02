from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import pyspark.sql.types as st

spark = SparkSession.builder.master("local[*]").appName("nested").getOrCreate()

data = [
    ("James", "Java"),
    ("Michael", "C++"),
    ("Robert", "Python")
]


array_data = [
  ("James", ["Java", "Scala", "C++"]),
  ("Michael", ["Spark", "Java", "C++"]),
  ("Robert", ["Python", "JS", "PHP"]),
  ("Max", [None, "JS", "PHP"])
]

df = spark.createDataFrame(
    data=array_data,
    schema=['name', 'progr_languages']
)

df.printSchema()
df.show(truncate=False)
print(df.count())   # 4

exploded_df = (
    df
    .withColumn("lang", sf.explode("progr_languages"))
)

exploded_df.printSchema()
exploded_df.show(truncate=False)
print(exploded_df.count())  # 12
