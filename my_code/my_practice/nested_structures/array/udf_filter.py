from decimal import Decimal
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

schema = st.StructType([
    st.StructField("id", st.StringType(), True),
    st.StructField("flat_status_code", st.IntegerType(), True),
    st.StructField(
        'status_codes', st.ArrayType(
            st.IntegerType(),
            True
        ),
        True
    )
])


data = [
    (
        '1',
        200,
        [200, 200]
    ),
    (
        '2',
        100,
        [100, 200]
    ),
    (
        '3',
        None,
        [200, 100]
    ),
    (
        '4',
        400,
        [400, 500]
    ),
(
        '5',
        200,
        [200, 200, 200]
    ),
    (
        '6',
        200,
        [200, 200, 200, 200]
    ),
    (
        '7',
        403,
        [400, 500, 200, 200]
    ),
    (
        '8',
        None,
        None
    ),
]

df = spark.createDataFrame(
    data=data,
    schema=schema
)


df.show()

flat_df = df.filter(sf.col("flat_status_code") == 200)

flat_df.show()

# df.show(truncate=False)
#
#
# @sf.udf(returnType=st.BooleanType())
# def all_200_udf(codes_array: Optional[List[int]]) -> bool:
#     # case if no used vehicle_ids for current vin_num (vehicle_ids is null)
#     # VRS API is not call for current row
#     if not codes_array:
#         return True
#     # vehicle_id is not null and call VRS API
#     return len(list(filter(lambda x: x != 200, codes_array))) == 0
#     # return len(codes_array) == len(list(filter(lambda x: x == 200, codes_array)))
#     # return not any(filter(lambda x: x != 200, codes_array))
#
#
# # all_200_udf = sf.udf(lambda x: all_200(x), st.BooleanType())
#
#
# filtered_df = df.withColumn(
#     'all_200',
#     all_200_udf(sf.col('status_codes'))
# ).filter(~sf.col("all_200"))
#
# filtered_df.show(truncate=False)


# df_exists = df.filter(
#     sf.exists(sf.col("status_codes"), lambda x: x != 200)
# )
# df_forall = df.filter(sf.forall(sf.col("status_codes"), lambda x: x != 200))
#
# df_exists.show(truncate=False)
