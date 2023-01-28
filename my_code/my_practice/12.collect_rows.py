from typing import List, Tuple, Dict, Union

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)


# data as list of Tuples
simpleData: List[Tuple[str, str, int]] = [
    ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
]

schema: List[str] = ["employee_name", "department", "salary"]

df = spark.createDataFrame(simpleData, schema)

df.show(truncate=False)
df.printSchema()

row_list: List[Row] = df.collect()
first_row: Row = df.collect()[0]

# Row to list:
first_row_list: List[Union[str, int]] = list(first_row)
# ['James', 'Sales', 3000]

# Row to dict:
first_row_dict: Dict[str, Union[str, int]] = first_row.asDict()
# {'employee_name': 'James', 'department': 'Sales', 'salary': 3000}

# receive value from 3 column 2 row:
second_salary_from_column_name: int = df.collect()[1]['salary']
second_salary_from_column_index: int = df.collect()[1][2]
print(
    second_salary_from_column_name,
    second_salary_from_column_index,
)
