from typing import List, Tuple

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
simpleData: List[Tuple[int, str, str, int]] = [
    (1, "James", "Sales", 4600),
    (2, "Michael", "Sales", 4100),
    (3, "Robert", "Sales", 3000),
    (4, "Maria", "Finance", 3000),
    (5, "James", "Sales", 3300),
    (6, "Scott", "Finance", 3900),
    (7, "Jen", "Finance", 3000),
    (8, "Jeff", "Marketing", 3000),
    (9, "Kumar", "Marketing", 2000),
    (10, "Saif", "Sales", 4100)
]

# schema: List[str] = ["id", "employee_name", "department", "salary"]
schema = st.StructType([
    st.StructField('id', st.IntegerType(), True),
    st.StructField('employee_name', st.StringType(), True),
    st.StructField('department', st.StringType(), True),
    st.StructField('salary', st.LongType(), True),
])

df = spark.createDataFrame(simpleData, schema)

df.show(truncate=False)
df.printSchema()

# count_distinct('salary')

print(
    "count_distinct:",
    df.select(sf.countDistinct("salary")).collect()[0][0]
)

# approx_count_distinct('salary')
print(
    "approx_count_distinct:",
    df.select(sf.approx_count_distinct("salary")).collect()[0][0]
)

agg_df = df.orderBy('id').select(
    sf.count('salary').alias('count_salary'),
    sf.countDistinct('salary').alias('count_dist_salary'),
    sf.approx_count_distinct('salary').alias('appr_count_dist_salary'),
    sf.avg('salary').alias('avg_salary'),
    sf.mean('salary').alias('mean_salary'),
    sf.min('salary').alias('min_salary'),
    sf.max('salary').alias('max_salary'),
    sf.sum('salary').alias('sum_salary'),
    sf.sumDistinct('salary').alias('sumDist_salary'),
    sf.collect_list('salary').alias('list_salary'),
    sf.collect_set('salary').alias('set_salary'),
    sf.first('salary').alias('first_salary'),
    sf.last('salary').alias('last_salary'),
    sf.kurtosis('salary').alias('kurt_salary'),
    sf.skewness('salary').alias('skew_salary'),
)

agg_df.printSchema()
agg_df.show()

# stddev(), stddev_samp(), stddev_pop()
# variance() = var_samp(), var_pop()