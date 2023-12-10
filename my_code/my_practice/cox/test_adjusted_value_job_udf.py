import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F
from botocore.exceptions import ClientError
import boto3
import json
import sys
from urllib.parse import unquote_plus
from awsglue.utils import getResolvedOptions
import requests
from awsglue.context import GlueContext
from awsglue.job import Job
import base64


# get secrets for vehicle catalog API
def get_secret(secret_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=args['AWS_REGION']
    )

    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )

    # Decrypts secret using the associated KMS CMK.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if get_secret_value_response.get('SecretString'):
        secret = get_secret_value_response['SecretString']
        secret_dict = json.loads(secret)
        return secret_dict
    else:
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return decoded_binary_secret


@F.udf(returnType=DoubleType())
def retrieveAdjustedValueUDF(
    vin_num,
    trim_api,
    book_key,
    mileage,
    sbmt_date
):

    if not mileage or not sbmt_date:
        return None

    if not book_key:
        book_key = _retrieve_book_key(vin_num, trim_api)
    adjusted_value = _retrieve_adjusted_value(
        vin_num,
        book_key,
        mileage,
        sbmt_date
    )
    return None if adjusted_value is None else adjusted_value


def _retrieve_book_key(vin_num, trim_api):
    time.sleep(0.005 * 5)
    vehicle_book_url = f"{BASE_URL}/chromeusa/vin"
    authorization_header = {'x-api-key': secret_dtv_key}
    retrieve_book_mapping = True

    vehicle_book_resp = requests.get(
        url=vehicle_book_url,
        params={
            'vin': vin_num,
            'trimHint': trim_api,
            'retrieveBookMapping': retrieve_book_mapping
        },
        headers=authorization_header
    )

    vehicle_book_key = None

    if vehicle_book_resp.status_code != 200 or vehicle_book_resp.json() == 0:  # or is none ???
        book_key = vehicle_book_key

    else:
        vehicle_book_resp_json = vehicle_book_resp.json()

        for book_mapping in vehicle_book_resp_json['Books'][0]['BookMappings']:
            if book_mapping.get('Name') == 'KelleyBlueBook':
                vehicle_book_key = book_mapping.get('VehicleBookKey')

        book_key = vehicle_book_key
    return book_key


def _retrieve_adjusted_value(
    vin_num,
    book_key,
    mileage,
    sbmt_date
):
    time.sleep(0.005 * 5)
    authorization_header = {'x-api-key': secret_dtv_key}

    adjusted_value = None

    if book_key:
        value_vehicle_url = f"{BASE_URL}/kbb/valuevehicle"
        value_vehicle_response = requests.get(
            url=value_vehicle_url,
            headers=authorization_header,
            params={
                "odometer": mileage,
                "vin": vin_num,
                "trimKey": book_key,
                "valuationDate": sbmt_date
            }
        )

        if value_vehicle_response.status_code == 200:
            for book in value_vehicle_response.json()['Books'][0]['BookValues']:
                if book.get('Name') == 'Retail':
                    adjusted_value = book.get("AdjustedValue")

    else:
        value_vehicle_url = f"{BASE_URL}/kbb/valuevehiclebyvin"
        value_vehicle_response = requests.get(
            url=value_vehicle_url,
            headers=authorization_header,
            params={
                "odometer": mileage,
                'vin': vin_num,
                'vehicleClass': 0,
                'valuationDate': sbmt_date
            }
        )
        adjusted_value = __validate_parse_book_response(value_vehicle_response)

    return adjusted_value


def __validate_parse_book_response(response: requests.Response):

    adjusted_value = None

    if response.status_code != 200 or response.json() == 0:
        return adjusted_value

    for book in response.json()['Books'][0]['BookValues']:
        if book.get('Name') == 'Retail':
            adjusted_value = book.get("AdjustedValue")

    return adjusted_value


# storing the final data in S3
def write_result(result_df, path):
    print("start writing function")

    result_df = result_df.coalesce(1)

    (
        result_df
        .write
        .partitionBy("year", "month", "day")
        .mode('overwrite')
        .parquet(path)
    )

    print("finish writing")

    # if result_df.rdd.isEmpty():
    #     print(f"Processing result for {folder_name} is empty. Training dataset can't be generated.")

    # approximate row size in bytes
    # row_size = 100
    # # desired max partition size in bytes
    # max_partition_size = 5 * 1000 * 1000 * 1000

    # if result_df.count() * row_size <= max_partition_size:
    #     # data fits into one partition
    #     print('fit in 1 partition - coalesce')
    #     df_w = result_df.coalesce(1)
    # else:
    #     # there is a possibility that data doesn't fit into one partition
    #     print('do not fit in 1 partition - repartition')
    #     df_w = result_df.repartition('year', 'month')

    # print("start coalesce")
    # df_w = result_df.coalesce(1)
    # print("finish coalesce")


if __name__ == '__main__':

    # Read the job arguments
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME',
            'AWS_REGION',
            'ProcessedBucketName',   # dri-fnibot-us-east-1-366490053584
            'BookKeysFolder',        # dealertrack/fni/ltv_history/book_keys
            'SourceFolder',          # dealertrack/fni/ltv_history/source_folder - copy file here from dealertrack/fni/output/fni_rate_estimation/total/
            'AdjustedValuesFolder',  # dealertrack/fni/ltv_history/adjusted_value_folder
            'DTVApiSecretName'
        ]
    )

    # Build spark session
    spark = (
        SparkSession
        .builder
        .appName("fni-data-pipeline")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    sc = spark.sparkContext
    WARN_MESSAGE_PREFIX = "[WARNING]"
    ERROR_MESSAGE_PREFIX = "[ERROR]"
    LOG_MSG_KEY = "LOG_MSG"
    glueContext = GlueContext(sc)

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    LOG = glueContext.get_logger()

    processed_bucket = args['ProcessedBucketName']
    book_keys_folder = args['BookKeysFolder']
    source_folder = args['SourceFolder']
    adjusted_value_folder = args['AdjustedValuesFolder']

    job_run_id = args['JOB_RUN_ID']
    job_name = args['JOB_NAME']
    run_id = args['JOB_RUN_ID']

    glue = boto3.client('glue')
    s3_client = boto3.client('s3')
    BASE_URL = "https://api.coxautoguidebooks.com"

    secret_dtv_key = get_secret(args["DTVApiSecretName"])["api_key"]

    # Reading data from source bucket

    print('start reading source files')

    source_df = spark.read.parquet(f"s3://{processed_bucket}/{source_folder}/").cache()

    book_keys_df = spark.read.parquet(f"s3://{processed_bucket}/{book_keys_folder}/")

    print('finished reading source files')

    if source_df and not source_df.rdd.isEmpty():

        vin_df = source_df.withColumn(
            "SBMT_date", F.to_date("SBMT_TS")
        ).select(
            "VIN_NUM",
            "TRIM_API",
            "MILEAGE",
            "SBMT_date"
        ).filter(
            F.col("VIN_NUM").isNotNull()
            & F.col("TRIM_API").isNotNull()
            & F.col("MILEAGE").isNotNull()
            & F.col("SBMT_date").isNotNull()
            & (F.year(F.col("SBMT_date")) == 2019)
            & (F.month(F.col("SBMT_date")) == 11)
        ).dropDuplicates()

        print("start joining")

        vin_with_book_keys_df = vin_df.join(
            book_keys_df,
            on=["VIN_NUM", "TRIM_API"],
            how='left'
        )

        print('before mapping', vin_with_book_keys_df.rdd.getNumPartitions())

        print('start mapping...')

        adjusted_value_df = vin_with_book_keys_df.withColumn(
            "ADJUSTED_VALUE",
            retrieveAdjustedValueUDF(
                F.col('VIN_NUM'),
                F.col('TRIM_API'),
                F.col('BOOK_KEY'),
                F.col('MILEAGE'),
                F.col('SBMT_date')
            )
        ).cache()

        print("count_adjusted_value", adjusted_value_df.count())
        adjusted_value_df.printSchema()

        # adjusted_value_df = adjusted_value_df.resolveChoice(specs=[('ADJUSTED_VALUE', 'cast:double')]).toDF()

        print('finish mapping...')

        print("after_mapping", adjusted_value_df.rdd.getNumPartitions())

        adjusted_value_df = (
            adjusted_value_df
            .select(
                "VIN_NUM",
                "TRIM_API",
                "MILEAGE",
                "SBMT_date",
                "ADJUSTED_VALUE"
            ).filter(F.col("ADJUSTED_VALUE").isNotNull())
            .withColumn("year", F.year(F.col("SBMT_date")))
            .withColumn("month", F.month(F.col("SBMT_date")))
            .withColumn("day", F.dayofmonth(F.col("SBMT_date")))
        ).cache()

        # print("conf set")

        # using overwrite mode as old partitions can be updated in result of deduplication process
        # spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # print("adjusted_value_df count", adjusted_value_df.count())
        # print("partition num", adjusted_value_df.rdd.getNumPartitions())
        # adjusted_value_df.printSchema()

        # print("coalesce")
        # coal_df = adjusted_value_df.coalesce(1)
        # print("coal_df count", coal_df.count())
        # print("coal_df partition num", coal_df.rdd.getNumPartitions())

        # print("repartition1")
        # repart_one_df = adjusted_value_df.repartition(1)
        # print("repart_one_df count", repart_one_df.count())
        # print("repart_one_df partnum", repart_one_df.rdd.getNumPartitions())

        # print("repartition1 year, month")
        # repart_ym_df = adjusted_value_df.repartition("year", "month")
        # print("repart_ym_df count", repart_ym_df.count())
        # print("repart_ym_df partition num", repart_ym_df.rdd.getNumPartitions())

        # write_result(adjusted_value_df, f"s3://{processed_bucket}/{adjusted_value_folder}")
        write_result(adjusted_value_df, f"s3://{processed_bucket}/awsdri/pv_tests/adjusted_value_udf")

    else:
        LOG.warn(f"{job_name}: {WARN_MESSAGE_PREFIX} No historic data were found.")

    job.commit()
