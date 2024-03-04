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
    ("Max", [None, "JS", "PHP"]),
    ("Mark", []),                               # will be lost in explode
    ("Slavik", None),                           # will be lost in explode
]

df = spark.createDataFrame(
    data=array_data,
    schema=['name', 'progr_languages']
)

df.printSchema()
df.show(truncate=False)
print(df.count())   # 6

exploded_df = (
    df
    # .withColumn("explode_lang", sf.explode("progr_languages"))        # rows with empty list will be lost
    .withColumn("explode_outer_lang", sf.explode_outer("progr_languages"))        # rows with empty list will be lost

)

exploded_df.printSchema()
exploded_df.show(truncate=False)
print(exploded_df.count())
# explode_count = 12
# explode_outer_count = 14