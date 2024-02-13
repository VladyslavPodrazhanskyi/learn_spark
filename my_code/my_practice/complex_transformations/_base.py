from typing import Callable

from pyspark.sql import DataFrame

from adl_iafa_glb_etl.interfaces import ITransformation


class BaseTransformation(ITransformation):
    def __init__(
        self,
        callee: Callable[[DataFrame], DataFrame],
    ):
        self._callee = callee

    def transform(self, df: DataFrame) -> DataFrame:
        return self._callee(df)
