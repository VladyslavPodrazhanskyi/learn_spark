from typing import List

from pyspark.sql import DataFrame

from adl_iafa_glb_etl.interfaces import ITransformation


class RemoveServiceColumns(ITransformation):

    def __init__(self, column_names: List[str]) -> None:
        self._column_names = column_names

    def transform(self, df: DataFrame) -> DataFrame:
        return df.drop(*self._column_names)
