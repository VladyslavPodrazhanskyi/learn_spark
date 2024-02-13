from typing import List

from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as sf
from pyspark.sql.types import DecimalType

from adl_iafa_glb_etl.entities import Join
from adl_iafa_glb_etl.extractors.factory import ExtractorFactory
from adl_iafa_glb_etl.interfaces import ITransformation
from adl_iafa_glb_etl.transformations import UnionTransformation

_INGESTION_DATE_COL: str = 'ingestion_date'
_INGESTION_TIME_COL: str = 'ingestion_time'


class AddDateTimeColumns(ITransformation):
    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn(_INGESTION_DATE_COL, sf.date_format(sf.current_timestamp(), 'y-MM-dd'))
            .withColumn(_INGESTION_TIME_COL, sf.date_format(sf.current_timestamp(), 'HH:mm:ss'))
        )


class AddCostCenterColumns(ITransformation):
    _table_name = 'dim_cost_center_text'
    _cost_center_selection = [
        'co_area',
        'costcenter',
        'txtmd',
        'dateto'
    ]

    def __init__(
        self,
        extractor_factory: ExtractorFactory,
    ) -> None:
        self._extractor_factory = extractor_factory

    def transform(self, df: DataFrame) -> DataFrame:
        selected = self._extractor_factory.create_selected_extractor(None, self._cost_center_selection)
        extractor = self._extractor_factory.create_filtered_extractor(selected, self._filter)
        cost_center_df = extractor.extract(self._table_name)
        return (
            df
            .join(cost_center_df, sf.col('cost_center_id').eqNullSafe(sf.col('costcenter')), 'left')
            .withColumn('cost_center_text', sf.col('txtmd'))
            .drop(*cost_center_df.columns)
        )

    @property
    def _filter(self) -> Column:
        return (
            (sf.col('co_area') == '7100')
            & (sf.col('dateto') == '9999.12.31')
        )


class AddCalculatedUsdAmountColumns(ITransformation):

    def __init__(
        self,
        extractor_factory: ExtractorFactory,
    ) -> None:
        self._extractor_factory = extractor_factory

    def transform(self, df: DataFrame) -> DataFrame:
        currency_rate_df = self._get_currency_rate_df()
        return (
            df.join(
                currency_rate_df,
                on=sf.col('transaction_currency_code').eqNullSafe(sf.col('from_currency'))
                   & sf.month(sf.col('transaction_date')).eqNullSafe(sf.col('month'))
                   & sf.year(sf.col('transaction_date')).eqNullSafe(sf.col('year')),
                how='left',
            )
            .withColumn(
                'transaction_amount_usd',
                sf.when(
                    sf.col('transaction_currency_code') == 'USD',
                    sf.col('transaction_amount_lc')
                ).otherwise(sf.col('transaction_amount_lc') * sf.col('direct_rate')).cast(DecimalType(20, 6))
            )
            .drop(*currency_rate_df.columns))

    def _get_currency_rate_df(self):
        base_df = self._extractor_factory.create_base_extractor().extract('dim_currency').cache()
        currency_df = (
            base_df
            .select(self._calendar_day_selection)
            .filter(
                sf.col('exchange_Rate_type').eqNullSafe(sf.lit('1XMA'))
                & sf.col('to_currency').eqNullSafe(sf.lit('USD'))
            ).groupBy(
                sf.col('from_currency'),
                sf.col('year'),
                sf.col('month')
            ).agg(sf.avg(sf.col('direct_rate')).alias('direct_rate'))
        )
        # to support new month day rate we will use rates from previous month
        for_current_day_df = (
            base_df
            .select(self._current_day_selection)
            .filter(
                sf.col('exchange_Rate_type').eqNullSafe(sf.lit('1XMA'))
                & sf.col('to_currency').eqNullSafe(sf.lit('USD'))
            ).filter(
                sf.when(
                    sf.month(sf.current_date()) == 1,
                    sf.year('calday_date').eqNullSafe(sf.year(sf.current_date()) - 1)
                    & (sf.month('calday_date') == 12)
                ).otherwise(
                    sf.year('calday_date').eqNullSafe(sf.year(sf.current_date()))
                    & sf.month('calday_date').eqNullSafe(sf.month(sf.current_date()) - 1)
                )
            ).groupBy(
                sf.col('from_currency'),
                sf.col('year'),
                sf.col('month')
            ).agg(sf.avg(sf.col('direct_rate')).alias('direct_rate'))
        )
        return UnionTransformation([currency_df]).transform(for_current_day_df)

    @property
    def _calendar_day_selection(self) -> List[Column]:
        return [
            sf.col('from_currency'),
            sf.col('direct_rate'),
            sf.year(sf.col('calday_date')).alias('year'),
            sf.month(sf.col('calday_date')).alias('month'),
        ]

    @property
    def _current_day_selection(self) -> List[Column]:
        return [
            sf.col('from_currency'),
            sf.col('direct_rate'),
            sf.year(sf.current_date()).alias('year'),
            sf.month(sf.current_date()).alias('month'),
        ]


class AddConcurPartitionColumns(ITransformation):

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn('creation_year', sf.year('creation_date'))
            .withColumn('creation_month', sf.month('creation_date'))
            .withColumn('creation_day', sf.dayofmonth('creation_date'))
        )


class AddCompanyCountryColumn(ITransformation):
    _table_name = 'company_region_codes'

    def __init__(
        self,
        extractor_factory: ExtractorFactory,
    ) -> None:
        self._extractor_factory = extractor_factory

    def transform(self, df: DataFrame) -> DataFrame:
        extractor = self._extractor_factory.create_selected_extractor(None, self._select)
        countries_df = extractor.extract(self._table_name)
        return (
            df.join(countries_df, sf.col('company_code') == sf.col('comp_code'), Join.left.value)
            .withColumn('company_country', sf.col('country_name'))
            .drop(*countries_df.columns)
        )

    @property
    def _select(self) -> List[Column]:
        return [
            sf.col('country').alias('country_name'),
            sf.col('company_code').alias('comp_code')
        ]
