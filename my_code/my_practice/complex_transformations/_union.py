from functools import reduce
from typing import List, Set

from pyspark.sql import DataFrame, types as T, functions as F

from adl_iafa_glb_etl.interfaces import ITransformation


class UnionTransformation(ITransformation):
    def __init__(
        self,
        base_dfs: List[DataFrame],
    ) -> None:
        self._base_dfs = base_dfs

    def transform(self, df: DataFrame) -> DataFrame:
        return reduce(
            lambda acc, inc: (
                _add_missing_columns(acc, inc.schema)
                .unionByName(
                    _add_missing_columns(inc, acc.schema),
                )
            ),
            self._base_dfs,
            df,
        )


def _add_missing_columns(
    df: DataFrame,
    target_columns: Set[T.StructField],
) -> DataFrame:
    current_columns_dict = {field.name: field for field in df.schema.fields}
    target_columns_dict = {field.name: field for field in target_columns}

    missing_columns = target_columns_dict.keys() - current_columns_dict.keys()

    for column in missing_columns:
        df = df.withColumn(column, F.lit(None).cast(target_columns_dict.get(column).dataType))
    return df
