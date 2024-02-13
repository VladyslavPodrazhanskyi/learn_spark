import itertools
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

from adl_iafa_glb_etl.entities import JoinDescription
from adl_iafa_glb_etl.interfaces import IExtractor, ITransformation


class JoinTransformation(ITransformation):

    def __init__(
        self,
        extractor: IExtractor,
        join_description: JoinDescription,
    ) -> None:
        self._extractor = extractor
        self._join_description = join_description

    def transform(self, df: DataFrame) -> DataFrame:
        right_df = self._prepare_right()
        joined_df = df.alias('left').join(
            right_df.alias('right'),
            on=self._join_description.on,
            how=self._join_description.how.value,
        )
        return joined_df.select(*self._select_columns(df))

    def _prepare_right(self):
        right_df = self._extractor.extract(self._join_description.table_name)
        for transformation in self._join_description.transformation_context.get_transformations():
            right_df = transformation.transform(right_df)
        return right_df

    def _select_columns(self, df: DataFrame) -> List[sf.Column]:
        left_columns = [
            sf.col('left.{name}'.format(name=field.name)).alias(field.name)
            for field in df.schema.fields
        ]
        right_columns = [
            sf.col('right.{name}'.format(name=name)).alias(name)
            for name in self._join_description.select
        ]
        return list(
            itertools.chain(
                left_columns,
                right_columns,
            ),
        )
