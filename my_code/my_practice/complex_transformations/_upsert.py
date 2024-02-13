from typing import List
from uuid import uuid4

from pyspark.sql import DataFrame, functions as F, Window

from adl_iafa_glb_etl.entities import Join
from adl_iafa_glb_etl.interfaces import ITransformation
from adl_iafa_glb_etl.transformations import UnionTransformation


class UpsertTransformation(ITransformation):
    _rank = '{uuid}_rank'.format(uuid=uuid4())

    def __init__(
        self,
        base_df: DataFrame,
        partition_column_names: List[str],
        key_column_names: List[str],
        ingestion_time_column_name: str,
    ) -> None:
        self._base_df = base_df
        self._partition_column_names = partition_column_names
        self._key_column_names = key_column_names
        self._ingestion_time_column_name = ingestion_time_column_name

    def transform(self, df: DataFrame) -> DataFrame:
        cached = df.cache()
        united = UnionTransformation([
            self._find_changes_in_base(cached),
        ]).transform(cached)
        return self._keep_recent(united)

    def _find_changes_in_base(self, df: DataFrame) -> DataFrame:
        return self._base_df.join(
            df.select(self._partition_column_names).distinct(),
            on=self._partition_column_names,
            how=Join.inner.value,
        )

    def _keep_recent(self, df: DataFrame) -> DataFrame:
        keys_window_ordered_by_ingestion = (
            Window
            .partitionBy(self._key_column_names)
            .orderBy(F.col(self._ingestion_time_column_name).desc())
        )

        return (
            df
            # drop duplicate to cover case when we run pipeline increment loading with already processed records
            # it's actual for test and dev environments only
            .drop_duplicates()
            .withColumn(self._rank, F.rank().over(keys_window_ordered_by_ingestion))
            .where(F.col(self._rank) == 1)
            .drop(self._rank)
        )
