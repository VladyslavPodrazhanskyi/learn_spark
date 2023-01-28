from pyspark.sql import DataFrame
import pyspark.sql.functions as sf


def add_date_time_cols(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn("ingestion_date", sf.date_format(sf.current_timestamp(), 'y-MM-dd'))
        .withColumn("ingestion_time", sf.date_format(sf.current_timestamp(), 'HH:mm:ss'))
    )
