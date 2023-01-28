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

itemsSchema = "id integer, name string, price float"
item_data = [
    (0, "Tomato", 2.0),
    (1, "Watermelon", 5.5),
    (2, "pineapple", 7.0)
]

ordersSchema = "id integer, itemid integer, count integer"
order_data = [
    (100, 0, 1),
    (100, 1, 1),
    (101, 2, 3),
    (102, 2, 8),
]

items: DataFrame = spark.createDataFrame(
    data=item_data,
    schema=itemsSchema
)
orders: DataFrame = spark.createDataFrame(
    data=order_data,
    schema=ordersSchema
)

print('items')
items.show()
items.printSchema()

print('orders')
orders.show()
orders.printSchema()


item_order_join = (
    items.join(
        orders,
        items.id == orders.itemid,
        'inner'
    )
).cache()

# item_order_join.explain()


y = (
    items.join(
        orders,
        items.id == orders.itemid,
        'inner'
    ).where(items.id == 2)
    .groupBy('name', 'price')
    .agg(sf.sum('count').alias('c'))
)

# y.explain(extended=True)
y.explain(mode='formatted')  #  extended, codegen, cost, formatted

