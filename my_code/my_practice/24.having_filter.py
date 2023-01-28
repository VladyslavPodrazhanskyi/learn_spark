from decimal import Decimal

from pyspark.sql import SparkSession, DataFrame, Row
import pyspark.sql.types as st
import pyspark.sql.functions as sf

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()
)

schema = st.StructType([
    st.StructField("id", st.IntegerType(), True),
    st.StructField("net", st.DecimalType(34, 5), True),
    st.StructField("gross", st.DecimalType(34, 5), True),
])


cur_df = spark.createDataFrame(
    [
        (1, Decimal('5'), Decimal('7')),
        (1, Decimal('5'), Decimal('5')),
        (1, Decimal('4'), Decimal('6')),
        (2, Decimal('5'), Decimal('7')),
        (2, Decimal('5'), Decimal('5')),
        (2, Decimal('4'), Decimal('-7')),
        (2, Decimal('4'), Decimal('-5')),
    ],
    schema=schema
)

cur_df.createOrReplaceTempView('cur')

print('sql_query')

spark.sql(
    """
    SELECT id,
    sum(net) net_sum,
    sum(gross) gross_sum,
    (sum(gross) - sum(net)) / sum(gross) deduction
    FROM cur
    Group by id 
    HAVING sum(gross) <> 0    
    """
).show()

print('spark_code_agg')

calculated_agg_df = (
    cur_df
    .groupBy('id')
    .agg(
        sf.sum('net').alias('net_sum'),
        sf.sum('gross').alias('gross_sum'),
        ((sf.sum('gross') - sf.sum('net')) / sf.sum('gross')).alias('deduction')
    ).filter(sf.sum('gross') != 0)
)

calculated_agg_df.show()

print('spark_code_with')

calculated_with_df = (
    cur_df
    .groupBy('id')
    .agg(
        sf.sum('net').alias('net_sum'),
        sf.sum('gross').alias('gross_sum'),
    ).filter(
        sf.sum('gross') != 0
    ).withColumn(
        'deduction',
        (sf.col('gross_sum') - sf.col('net_sum')) / sf.col('gross_sum')
    )
)

calculated_with_df.show()






