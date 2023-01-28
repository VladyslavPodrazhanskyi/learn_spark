import os
from pyspark import SparkContext, SparkConf
from my_code import ROOT

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
    return name, gender, salary


func_rdd = df.rdd.map(lambda row: func(row))

map_df = func_rdd.toDF(['name', "gender", "salary"])

map_df.show()
