import abc
import itertools
from dataclasses import dataclass
from typing import List, Optional, Generic, TypeVar

from pyspark.sql import types as st, DataFrame


# TODO: Split module by area of responsibilities. (To make package)

@dataclass(frozen=True)
class Column:
    name: str
    type: st.DataType


class BaseColumns:
    @classmethod
    def all(cls) -> List[Column]:
        return [
            attr
            for attr in vars(cls).values()  # noqa: WPS421
            if isinstance(attr, Column)
        ]

    @classmethod
    def any(cls) -> bool:
        return (
            bool([
                attr
                for attr in vars(cls).values()  # noqa: WPS421
                if isinstance(attr, Column)
            ])
        )


class BaseTable:
    name = ''

    columns = BaseColumns
    partition_columns = BaseColumns

    @classmethod
    def all(cls, drop: Optional[List[Column]] = None) -> List[Column]:
        to_drop: List[Column] = drop if drop else []
        return list(
            filter(
                lambda column: column not in to_drop,
                itertools.chain(
                    cls.columns.all(),
                    cls.partition_columns.all(),
                ),
            ),
        )


class IExtractor(abc.ABC):
    @abc.abstractmethod
    def extract(self, table_name: str) -> DataFrame:
        pass


class ILoader(abc.ABC):
    @abc.abstractmethod
    def load(self, df: DataFrame, table_name: str) -> None:
        pass


class ITransformation(abc.ABC):
    @abc.abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass


class ITransformationContext(abc.ABC):
    @abc.abstractmethod
    def get_transformations(self) -> List[ITransformation]:
        pass


TA = TypeVar('TA')


class IArgumentParser(Generic[TA]):
    def parse(self, manual_args: Optional[List[str]]) -> TA:
        """To parse incoming inputs from CLI."""


class IEtl(abc.ABC):
    @abc.abstractmethod
    def run(self) -> None:
        """Abstract interface for all ETLs."""


class IEtlBuilder(abc.ABC):
    @abc.abstractmethod
    def build(self) -> List[IEtl]:
        """Abstract interface for all ETL builders."""


class IMetaStoreClient(abc.ABC):
    @abc.abstractmethod
    def is_table_exists(self, table_name: str) -> bool:
        pass


class IApplier(abc.ABC):
    @abc.abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        pass


class IStorageClient(abc.ABC):
    @abc.abstractmethod
    def delete_folder(self, folder_name: str) -> None:
        pass
