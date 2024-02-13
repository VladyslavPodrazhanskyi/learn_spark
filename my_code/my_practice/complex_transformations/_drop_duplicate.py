from typing import Optional, List

from pyspark.sql import DataFrame

from adl_iafa_glb_etl.interfaces import ITransformation


class DropDuplicates(ITransformation):

    def __init__(self, pk_column: Optional[List[str]] = None) -> None:
        self._pk_column = pk_column

    def transform(self, df: DataFrame) -> DataFrame:
        if self._pk_column:
            return df.drop_duplicates(self._pk_column)
        return df
