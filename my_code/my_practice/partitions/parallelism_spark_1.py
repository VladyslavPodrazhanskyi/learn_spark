import pandas as pd
import numpy as np

import time
from pprint import pprint

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

from my_code import ROOT

spark = SparkSession.builder.master("local[*]").getOrCreate()


length = 100
names = np.random.choice(['Bob', 'James', 'Marek', 'Johannes', None], length)

amounts = np.random.randint(0, 1000000, length)
country = np.random.choice(
	['United Kingdom', 'Poland', 'USA', 'Germany', None],
	length
)
df = pd.DataFrame({'name': names, 'amount': amounts, 'country': country})

transactions = spark.createDataFrame(df)
print('Number of partitions: {}'.format(transactions.rdd.getNumPartitions()))
print('Partitioner: {}'.format(transactions.rdd.partitioner))
print('Partitions structure: {}'.format(transactions.rdd.glom().collect()))

transactions.show()
