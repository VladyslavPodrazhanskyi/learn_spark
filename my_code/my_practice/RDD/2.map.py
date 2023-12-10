"""
    PySpark provides map(), mapPartitions()
    to loop/iterate through rows in RDD/DataFrame
    to perform the complex transformations,
    and these two returns the same number of records as in the original DataFrame
    but the number of columns could be different (after add/update).

    Mostly for simple computations,
    instead of iterating through using map() and foreach(),
    you should use either DataFrame select() or DataFrame withColumn()
    in conjunction with PySpark SQL functions.

    PySpark also provides foreach() & foreachPartitions() actions
    to loop/iterate through each Row in a DataFrame but these two returns nothing

"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('rdd').getOrCreate()
sc = spark.sparkContext

data = [
    "Project",
    "Gutenberg’s",
    "Alice’s",
    "Adventures",
    "in",
    "Wonderland",
    "Project",
    "Gutenberg’s",
    "Adventures",
    "in",
    "Wonderland",
    "Project",
    "Gutenberg’s"
]

rdd = sc.parallelize(data)

rdd2 = rdd.map(lambda x: (x, 1))

for el in rdd2.collect():
    print(el)


data = [
    ('James', 'Smith', 'M', 30),
    ('Anna', 'Rose', 'F', 41),
    ('Robert', 'Williams', 'M', 62),
]

columns_schema = ["firstname", "lastname", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns_schema)
df.show()

# map_rdd = df.rdd.map(lambda x: (x[0] + ', ' + x[1], x[2], x[3]))

# ref_name_rdd = df.rdd.map(lambda x: (x['firstname'] + ', ' + x['lastname'], x['gender'].lower(), x['salary']))


def func(row):
    firstname = row.firstname
    lastname = row.lastname
    name = firstname + ', ' + lastname
    gender = row.gender.lower()
    salary = row.salary
    return tuple(row.asDict().keys())


func_rdd = df.rdd.map(lambda row: func(row))

print(func_rdd.partitioner)   # None

map_df = func_rdd.toDF(['name', "gender", "salary"])

map_df.show()
