from pprint import pprint
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as T
import pyspark.sql.functions as F

# from utils import get_project_root
from code.utils import get_project_root

ROOT = get_project_root()
print(ROOT)


spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


df1 = spark.createDataFrame([
    Row(id=1, value=25),
    Row(id=2, value=None),
    Row(id=2, value=-1)
])

df1.show()

df1.select(
    df1['value'].eqNullSafe(-1)
).show()