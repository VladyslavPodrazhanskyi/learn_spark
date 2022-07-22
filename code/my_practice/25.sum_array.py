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

spark.sparkContext.setLogLevel("INFO")

schema = st.StructType([
    st.StructField("id", st.StringType(), True),
    st.StructField('journalentry', st.ArrayType(
        st.StructType([
            st.StructField('journalid', st.StringType(), True),
            st.StructField('amount', st.IntegerType(), True),
        ]),
        True), True)
])


data = [
        (
            'id_278298sh',
            [
                ('journalid_1', 5),
                ('journalid_3', 7),
            ]
        ),
        (
            'id_2782ds98sh',
            [
                ('journalid_4', 5),
            ]
        ),
        (
            'id_2782ds98sh',
            []
        )

    ]

df = spark.createDataFrame(
    data=data,
    schema=schema
)

df.withColumn(
    'sum_amount',
    sf.aggregate(sf.col('journalentry.amount'), sf.lit(0.0), lambda x, y: x + y)
).show(truncate=False)

df.filter(sf.col('id') == 'id_278298sh')

