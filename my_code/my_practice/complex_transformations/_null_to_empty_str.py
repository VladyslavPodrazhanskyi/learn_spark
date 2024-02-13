from functools import reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

from adl_iafa_glb_etl.interfaces import ITransformation


class NullToEmptyStrTransformation(ITransformation):

    def __init__(self, columns: List[str] = None) -> None:
        self._columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        return reduce(
            lambda init_df, column: (
                init_df.withColumn(
                    column,
                    sf.trim(
                        sf.when(sf.col(column).isNull(), '').otherwise(sf.col(column))
                    )
                )
            ),
            self._columns,
            df,
        )
