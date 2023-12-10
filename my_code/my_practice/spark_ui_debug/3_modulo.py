from typing import Callable

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as st
import pyspark.sql.functions as sf
from my_code import ROOT

# http://192.168.100.3:4040/


spark = (
    SparkSession
        .builder
        .master("local[*]")
        .getOrCreate()
)

data = [
    (1, 2, 4, 1),
    (3, 6, 5, 0),
    (9, 4, None, 1),
    (11, 17, None, 1),
]

df = spark.createDataFrame(
    data=data,
    schema=['one', 'two', 'three', 'four']
)


def modulo(
    df: DataFrame,
    old_col: str,
    new_col: str,
    mod: int
) -> DataFrame:
    return df.withColumn(
        new_col,
        sf.col(old_col) % mod
    )


modulo(df, 'one', 'module', 5).show()


def outer_modulo(
    old_col: str,
    new_col: str,
    mod: int
) -> Callable[[DataFrame], DataFrame]:

    def _inner_func(df: DataFrame) -> DataFrame:
        return df.withColumn(
            new_col,
            sf.col(old_col) % mod
        )

    return _inner_func


# use wit transform

# Transform-enabled functions are not necessary to write performant and maintainable
# programs. On the other hand, they enable us to embed arbitrary logic through the
# transform method, which keeps the method-chaining my_code organization pattern. This
# yields cleaner, more readable my_code.


df.transform(
    outer_modulo('one', 'one_mod2', 2)
).transform(
    outer_modulo('two', 'two_mod3', 3)
).show()

# use without transform

outer_modulo('one', 'one_mod2', 2)(df).show()



