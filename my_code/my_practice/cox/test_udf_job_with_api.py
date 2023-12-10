import sys
import time
import requests
from random import choice

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType
from pyspark import StorageLevel
import datetime as dt
import boto3
import json
import base64

from pyspark.ml.feature import Bucketizer, QuantileDiscretizer


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


# Read the job arguments
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'DataScienceBucketName',
        'DataScienceFolder',
        'BookKeysFolder',  # add to yaml file    dealertrack/fni/ltv_history/book_keys
        'HistoryFolder',  # add to yaml file    dealertrack/fni/ltv_history/adjusted_value
        'CatalogDataBaseName',
        'CatalogTableName',
        # 'DTVApiSecretName'
    ]
)

BASE_URL = "https://api.coxautoguidebooks.com"

secret_dtv_key = get_secret(args["DTVApiSecretName"])["api_key"]

# book_keys = ['book_key_1', 'book_key_2', 'book_key_3', None]
# adj_values = [10000.0, 20000.0, None]


data_science_bucket_name = args['DataScienceBucketName']
data_science_folder = args['DataScienceFolder']

database_name = args["CatalogDataBaseName"]
table_name = args["CatalogTableName"]

hist_folder = args["HistoryFolder"]
book_keys_folder = args["BookKeysFolder"]

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
LOG = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job_name = args['JOB_NAME']
WARN_MESSAGE_PREFIX = "[WARNING]"

s3_client = boto3.client('s3')

# Static and validation part

statistic_thresholds = {
    "penske": {
        'term': {'12': 0.004, '24': 0.12, '36': 1.537, '48': 3.969, '60': 17.953, '72': 56.107, '84': 4.872},
        'loanToValueRatio': {
            (0.0, 78.14): 9.996, (78.14, 92.49): 10.0, (92.49, 103.07): 9.999, (103.07, 124.81): 20.004,
            (124.81, 130.0): 2.777, (130.0, 135.0): 28.751, (135.0, 174.282): 18.473, (174.282, 200): 0.0},
        'vehicleAge': {'0': 4.062, '1': 15.285, '2': 17.804, '3': 28.708, '4': 14.104, '5': 7.497, '6': 5.309,
                       '7': 3.399, '8': 1.843, '9': 0.907, '10': 0.48, '11': 0.221, '12': 0.129},
        'salaryAnnual': {(3500, 20000): 1.946, (20000, 50000): 35.532, (50000, 100000): 43.993,
                         (100000, 150000): 10.855,
                         (150000, 200000): 3.715, (200000, 250000): 1.434, (250000, 300000): 0.595,
                         (300000, 400000): 0.778, (400000, 1000000): 0.87},
        'cashDownAmount': {(0, 5000): 76.04, (5000, 10000): 15.8, (10000, 15000): 5.362, (15000, 20000): 1.562},
        'estimatedAmountFinanced': {(5000, 10000): 6.525, (10000, 20000): 43.947, (20000, 40000): 43.947,
                                    (40000, 80000): 6.023, (80000, 120000): 0.173},
        'mileage': {(0, 10000): 9.633, (10000, 20000): 14.504, (20000, 40000): 36.77, (40000, 80000): 30.451,
                    (80000, 100000): 5.871, (100000, 150000): 2.563},
        'state': {
            'OK': 2.068, 'CA': 6.811, 'MN': 1.253, 'NH': 0.745, 'OH': 5.238, 'WI': 1.084, 'ND': 0.331, 'AP': 0.0,
            'AZ': 1.353, 'NM': 0.447, 'FL': 9.326, 'AL': 1.25, 'AR': 1.606, 'KY': 1.532, 'KS': 0.683, 'LA': 1.144,
            'NY': 6.275, 'PR': 0.001, 'UT': 0.536, 'WA': 1.198, 'DE': 0.512, 'ME': 0.387, 'AK': 0.078, 'UNK': 0.017,
            'SC': 1.832, 'NC': 3.853, 'IA': 0.566, 'RI': 0.601, 'VT': 0.244, 'GU': 0.0, 'NJ': 5.076, 'IL': 4.758,
            'WY': 0.193, 'IN': 2.132, 'CT': 1.588, 'WV': 0.702, 'MT': 0.115, 'ID': 0.253, 'SD': 0.208, 'AE': 0.0,
            'MI': 1.166, 'OR': 0.656, 'MS': 0.732, 'CO': 1.501, 'MD': 1.778, 'VA': 1.896, 'NE': 0.453, 'PA': 8.212,
            'TN': 2.26, 'DC': 0.096, 'GA': 3.253, 'MO': 1.93, 'TX': 6.234, 'MA': 2.982, 'NV': 0.6, 'HI': 0.257},
        'lowScore': {(660, 680.0): 12.451, (680.0, 710.0): 17.68, (710.0, 730.0): 10.979, (730.0, 750.0): 10.035,
                     (750.0, 800.0): 20.616, (800.0, 900.0): 22.015},
        'total_count': {'*': 1913667}
    },
    "total": {
        'term': {'12': 0.005, '24': 0.091, '36': 0.941, '48': 2.958, '60': 13.55, '72': 62.243, '84': 5.401},
        'loanToValueRatio': {(0.0, 77.64): 10.134, (77.64, 90.19): 10.088, (90.19, 98.53): 9.982,
                             (98.53, 104.23): 9.873, (104.23, 110.5): 10.177,
                             (110.5, 115.0): 6.113, (115.0, 120.0): 13.232, (120.0, 125.79): 10.618,
                             (125.79, 130.0): 2.177, (130.0, 174.29): 17.603},
        'vehicleAge': {'0': 2.802, '1': 12.802, '2': 15.681, '3': 23.842, '4': 14.898, '5': 9.306,
                       '6': 7.064, '7': 4.899, '8': 3.302, '9': 2.044, '10': 1.184, '11': 0.72, '12': 0.53},
        'salaryAnnual': {(3500, 20000): 3.041, (20000, 50000): 49.919, (50000, 100000): 36.685,
                         (100000, 150000): 6.373, (150000, 200000): 1.862, (200000, 250000): 0.672,
                         (250000, 300000): 0.27, (300000, 400000): 0.366, (400000, 1000000): 0.474},
        'cashDownAmount': {(0, 5000): 85.776, (5000, 10000): 10.103, (10000, 15000): 2.721, (15000, 20000): 0.774},
        'estimatedAmountFinanced': {(5000, 10000): 7.487, (10000, 20000): 46.59, (20000, 40000): 40.08,
                                    (40000, 80000): 5.37, (80000, 120000): 0.145},
        'mileage': {(0, 10000): 6.219, (10000, 20000): 10.443, (20000, 40000): 30.826, (40000, 80000): 35.543,
                    (80000, 100000): 9.404,
                    (100000, 150000): 6.219},
        'state': {'NJ': 3.275, 'IL': 4.605, 'OK': 2.489, 'CA': 5.021, 'WY': 0.211, 'IN': 2.513, 'CT': 1.167,
                  'WV': 0.612, 'MN': 0.93, 'NH': 0.427, 'OH': 4.199, 'WI': 1.263, 'MT': 0.094, 'ND': 0.198, 'AP': 0.0,
                  'AZ': 1.36, 'ID': 0.618, 'SD': 0.182, 'AE': 0.0, 'MI': 0.957, 'NM': 0.831, 'OR': 0.733, 'MS': 0.822,
                  'CO': 1.312, 'FL': 11.715, 'AL': 2.353, 'AR': 1.461, 'KY': 1.662, 'KS': 0.615, 'LA': 1.447,
                  'NY': 5.088, 'MD': 1.787, 'VA': 2.478, 'NE': 0.442, 'PR': 0.0, 'PA': 5.843, 'UT': 0.602,
                  'WA': 1.987, 'DE': 0.396, 'ME': 0.311, 'AK': 0.049, 'UNK': 0.019, 'SC': 2.323, 'TN': 2.447,
                  'DC': 0.121, 'GA': 4.909, 'MO': 1.822, 'NC': 4.795, 'IA': 0.777, 'TX': 7.679, 'RI': 0.34, 'MA': 1.815,
                  'NV': 0.554, 'HI': 0.199, 'VT': 0.139, 'GU': 0.003},
        'lowScore': {(299.999, 530.0): 8.326, (530.0, 570.0): 9.596, (570.0, 600.0): 9.406, (600.0, 630.0): 11.23,
                     (630.0, 650.0): 7.943, (650.0, 680.0): 12.049, (680.0, 710.0): 10.712, (710.0, 740.0): 9.143,
                     (740.0, 790.0): 10.914, (790.0, 800.0): 1.645},
        'total_count': {'*': 15298453}
    }
}

statistic_relative_errors = {
    'term': 0.30,
    'loanToValueRatio': 0.30,
    'vehicleAge': 0.30,
    'salaryAnnual': 0.30,
    'cashDownAmount': 0.30,
    'estimatedAmountFinanced': 0.30,
    'mileage': 0.30,
    'state': 0.30,
    'lowScore': 0.30,
    'total_count': 0.40
}


# Example of calculation ranges for one field
def calculate_ranges():
    qd = QuantileDiscretizer(numBuckets=10, relativeError=0.0001, inputCol="loanToValueRatio", outputCol="result")
    print(qd.fit(df).getSplits())


def calculate_quantile(df, columns, quantile):
    # If relativeError set to zero, the exact quantiles are computed, which could be very expensive
    # (causes all the values to be passed to driver)
    quantile_values = df.approxQuantile(columns, [quantile], (1 - quantile) / 20)
    res = {}
    for i, col_name in enumerate(columns, start=0):
        res[col_name] = quantile_values[i][0]
    return res


def standardize_ltv(
    main_df: DataFrame,
    historical_df: DataFrame,
    bk_keys_df: DataFrame,
) -> DataFrame:
    # rename old ltv and prepare df for enrichment
    prepared_df = (
        main_df
        .withColumnRenamed("LOAN_TO_VAL_RT", "dt_ltv")
        .withColumn("sbmt_date", F.to_date("sbmt_ts"))
    ).cache()

    print("prepared_df")
    prepared_df.printSchema()

    # enrich df with historical adjusted_value and book_keys
    hist_enriched_df = prepared_df.join(
        historical_df,
        on=['VIN_NUM', 'TRIM_API', 'MILEAGE', 'SBMT_date'],
        how='left'
    ).join(
        bk_keys_df,
        on=['VIN_NUM', 'TRIM_API', ],
        how='left'
    ).cache()

    print("hist_enriched_df")
    hist_enriched_df.printSchema()

    api_enriched_df = hist_enriched_df.withColumn(
        "ADJUSTED_VALUE",
        F.when(
            F.col('ADJUSTED_VALUE').isNull(),
            retrieveAdjustedValueUDF(
                F.col('VIN_NUM'),
                F.col('TRIM_API'),
                F.col('EST_AMT_FINANCED'),
                F.col('BOOK_KEY'),
                F.col('MILEAGE'),
                F.col('SBMT_date')
            )
        ).otherwise(F.col("ADJUSTED_VALUE"))
    )

    print("hist_enriched_df - api_enriched_df")
    print(set(hist_enriched_df.schema) - set(api_enriched_df.schema))

    print("api_enriched_df - hist_enriched_df")
    print(set(api_enriched_df.schema) - set(hist_enriched_df.schema))

    # calculate standartize LTV and use old LTV if adjusted_value is null
    # delete service cols to keep the same schema ( leave adjusted value)
    # as indicator which LTV is used
    ltv_df = calculate_ltv(api_enriched_df).cache()

    print("ltv_df")
    ltv_df.printSchema()

    return ltv_df


@F.udf(returnType=DoubleType())
def retrieveAdjustedValueUDF(
    vin_num,
    trim_api,
    est_amt_financed,
    book_key,
    mileage,
    sbmt_date
):
    if not est_amt_financed or not mileage or not sbmt_date:
        return None

    if not book_key:
        book_key = _retrieve_book_key(vin_num, trim_api)
    adjusted_value = _retrieve_adjvalue_from_api(
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


def _retrieve_adjvalue_from_api(
    vin_num,
    book_key,
    mileage,
    sbmt_date
):

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


def calculate_ltv(main_df: DataFrame) -> DataFrame:
    ltv_df = main_df.withColumn(
        "LOAN_TO_VAL_RT",
        F.when(
            F.col("ADJUSTED_VALUE").isNotNull()
            & (F.col("ADJUSTED_VALUE").isNotNull() != 0)
            & F.col("EST_AMT_FINANCED").isNotNull(),
            (F.col("EST_AMT_FINANCED") * 100 / F.col("ADJUSTED_VALUE")).cast(DoubleType())
        ).otherwise(F.col("dt_ltv"))
    ).drop('sbmt_date', 'book_key')

    return ltv_df


# reading enriched dealer track data
start_date = dt.date.today() - dt.timedelta(days=5)

df = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="datasource0",
    additional_options={"groupFiles": "inPartition", "groupSize": "128000000"},
    push_down_predicate=f"to_date(concat(year, '-', month, '-', day)) >= '{start_date}'"
).toDF()

# TODO temporary solution to filter junk data
df = df.filter(
    df.SBMT_TS >= dt.date(year=2019, month=1, day=1)
).filter(
    F.length(F.col('vin_num')) == 17
).sample(0.01).cache()

print("after reading and sampling")
print(df.count())
df.printSchema()

history_df = spark.read.parquet(
    f"s3://{data_science_bucket_name}/{hist_folder}/"
).withColumn(
    'ADJUSTED_VALUE',
    F.lit(10000).cast(DoubleType())
).drop("year", "month", "day").cache()

print("history_df")
print(history_df.count())
print(history_df.printSchema())

book_keys_df = spark.read.parquet(f"s3://{data_science_bucket_name}/{book_keys_folder}/").cache()

print("book_keys_df")
print(book_keys_df.count())
print(book_keys_df.printSchema())

# Original LENDER_ID is not available
df = df.withColumn('LENDER_ID', df.HASHED_LENDER_ID)

# Taking data for RETAIL
# removes __ rows
df = df.filter(df.DEAL_TYP_CD.isin(['R', 'RETL']))

# Removing Buying Spectrum= 'Unknown'. Also removes rows with High Score = 299 and 9999
# removes __ rows
df = df.filter(~F.col('BUYING_SPECTRUM').eqNullSafe('Unknown'))

# Limiting the range of Low/High Scores between 300 and 900 while keeping the nulls
# removes __ rows
df = df.filter(df.LOW_SCORE.between(300, 900) | df.LOW_SCORE.isNull())
# removes __ rows
df = df.filter(df.HIGH_SCORE.between(300, 900) | df.HIGH_SCORE.isNull())

# Replacing Nulls in Customer Rate with Contract Rate
df = df.withColumn('CUST_RATE', F.coalesce(df.CUST_RATE, df.CONTRACT_RATE))

# Removing rows with rates less than 0. Keeping Null Values for now
df = df.filter((df.BUY_RATE >= 0) | (df.BUY_RATE.isNull())) \
    .filter((df.CUST_RATE >= 0) | (df.CUST_RATE.isNull())) \
    .filter((df.CONTRACT_RATE >= 0) | (df.CONTRACT_RATE.isNull()))


def deduplicate_by_timestamp(input_df, key_columns, timestamp_column):
    window_spec = Window.partitionBy([F.col(key_col) for key_col in key_columns]).orderBy(
        F.col(timestamp_column).desc())
    return input_df.withColumn("ROW_NUM", F.row_number().over(window_spec)).filter(F.col("ROW_NUM") == 1) \
        .drop("ROW_NUM")


df = deduplicate_by_timestamp(df, ["CREDIT_APP_ID"], "UPLOAD_DATE")

print("after_deduplication")
print(df.count())
df.printSchema()

ded_schema = df.schema

df = standardize_ltv(df, history_df, book_keys_df).cache()

print("after_standardize_ltv")
print(df.count())
df.printSchema()
stand_ltv_schema = df.schema

print('ded - stand')
print()
print(set(ded_schema) - set(stand_ltv_schema))
print()
print('stand  dep')
print()
print(set(stand_ltv_schema) - set(ded_schema))


df = df.repartition(df.DEAL_ID)

df = df.persist(StorageLevel.MEMORY_AND_DISK)

# Removing rows with rate values greater than 99th Percentile
# removes __ rows
quantiles = calculate_quantile(df, ["BUY_RATE", "CONTRACT_RATE", "CUST_RATE"], 0.99)

df = df.filter((df.BUY_RATE <= quantiles['BUY_RATE']) | (df.BUY_RATE.isNull()))
df = df.filter((df.CUST_RATE <= quantiles['CUST_RATE']) | (df.CUST_RATE.isNull()))
df = df.filter((df.CONTRACT_RATE <= quantiles['CONTRACT_RATE']) | (df.CONTRACT_RATE.isNull()))

# Removing rows with negative values for certain columns. Trade In amount and Disbursed amount (not completely sure)
# are allowed to have negative values. Also Keeping the null values for now
# removes __ rows
df = df.filter((df.AMT_FINANCED >= 0) | (df.AMT_FINANCED.isNull())) \
    .filter((df.APRV_LOAN_AMT >= 0) | (df.APRV_LOAN_AMT.isNull())) \
    .filter((df.BUY_RATE >= 0) | (df.BUY_RATE.isNull())) \
    .filter((df.CASH_DOWN_AM >= 0) | (df.CASH_DOWN_AM.isNull())) \
    .filter((df.CONTRACT_RATE >= 0) | (df.CONTRACT_RATE.isNull())) \
    .filter((df.DOWN_PAYMENT >= 0) | (df.DOWN_PAYMENT.isNull())) \
    .filter((df.EST_PAYMENT >= 0) | (df.EST_PAYMENT.isNull())) \
    .filter((df.LOAN_TO_VAL_RT >= 2) | (df.LOAN_TO_VAL_RT.isNull())) \
    .filter((df.MONTHLY_PMT >= 0) | (df.MONTHLY_PMT.isNull())) \
    .filter((df.OFFICIAL_FEES >= 0) | (df.OFFICIAL_FEES.isNull())) \
    .filter((df.PAYMT_AM >= 0) | (df.PAYMT_AM.isNull())) \
    .filter((df.SCRTY_DEPOSIT_AM >= 0) | (df.SCRTY_DEPOSIT_AM.isNull())) \
    .filter((df.CON_ACQUISITION_FEE_AM >= 0) | (df.CON_ACQUISITION_FEE_AM.isNull())) \
    .filter((df.CUST_MONEY_FCTR_AM >= 0) | (df.CUST_MONEY_FCTR_AM.isNull())) \
    .filter((df.DLR_BONUS_AM >= 0) | (df.DLR_BONUS_AM.isNull())) \
    .filter((df.DLR_PCTPN_AM >= 0) | (df.DLR_PCTPN_AM.isNull())) \
    .filter((df.GAP_DLR_AMT >= 0) | (df.GAP_DLR_AMT.isNull())) \
    .filter((df.LENDER_MONEY_FCTROR_AM >= 0) | (df.LENDER_MONEY_FCTROR_AM.isNull())) \
    .filter((df.PAYMENT_CALL_AMT >= 0) | (df.PAYMENT_CALL_AMT.isNull()))

# Removing Rows where EST_PAYMENT_AMT is greater than EST_FINANCE_AMT
df = df.filter((df.EST_AMT_FINANCED >= df.EST_PAYMENT) | (df.EST_AMT_FINANCED.isNull()) | (df.EST_PAYMENT.isNull()))

df = df.persist(StorageLevel.MEMORY_AND_DISK)

quantiles_99 = calculate_quantile(df, ["EST_PAYMENT"], 0.99)
quantiles_999 = calculate_quantile(df, ["AMT_FINANCED", "APRV_LOAN_AMT", "EST_AMT_FINANCED", "CASH_DOWN_AM"], 0.999)
quantiles_9995 = calculate_quantile(df, ["GAP"], 0.9995)
quantiles_9999 = calculate_quantile(df, ["INVOICE_AM", "MONTHLY_PMT"], 0.9999)

df = df.filter((df.APRV_LOAN_AMT <= quantiles_999['APRV_LOAN_AMT']) | df.APRV_LOAN_AMT.isNull()) \
    .filter((df.AMT_FINANCED <= quantiles_999['AMT_FINANCED']) | (df.AMT_FINANCED.isNull())) \
    .filter((df.EST_AMT_FINANCED <= quantiles_999['EST_AMT_FINANCED']) | df.EST_AMT_FINANCED.isNull()) \
    .filter((df.AUTO_YEAR != 9999) | df.AUTO_YEAR.isNull()) \
    .filter((df.CASH_DOWN_AM <= quantiles_999['CASH_DOWN_AM']) | df.CASH_DOWN_AM.isNull()) \
    .filter((df.EST_PAYMENT <= quantiles_99['EST_PAYMENT']) | df.EST_PAYMENT.isNull()) \
    .filter((df.GAP <= quantiles_9995['GAP']) | df.GAP.isNull()) \
    .filter((df.INVOICE_AM <= quantiles_9999['INVOICE_AM']) | df.INVOICE_AM.isNull()) \
    .filter((df.MONTHLY_PMT <= quantiles_9999['MONTHLY_PMT']) | df.MONTHLY_PMT.isNull())

# removing additional values and outliers based on some business discussions

df = df.persist(StorageLevel.MEMORY_AND_DISK)

quantiles_995 = calculate_quantile(df, ['TERM', 'LOAN_TO_VAL_RT'], 0.995)
quantiles_999 = calculate_quantile(df, ['SALARY_ANNUAL', 'MORTGAGE_PAYMT_OR_RENT_AM'], 0.999)

df = df.filter(~df.TERM.eqNullSafe(0)) \
    .filter((df.TERM <= quantiles_995['TERM']) | (df.TERM.isNull())) \
    .filter(~df.LOAN_TO_VAL_RT.eqNullSafe(0)) \
    .filter((df.LOAN_TO_VAL_RT <= quantiles_995['LOAN_TO_VAL_RT']) | (df.LOAN_TO_VAL_RT.isNull())) \
    .filter(~df.MONTHS_EMPLOYED.eqNullSafe(0)) \
    .filter((df.SALARY_ANNUAL <= quantiles_999['SALARY_ANNUAL']) | (df.SALARY_ANNUAL.isNull())) \
    .filter((df.MORTGAGE_PAYMT_OR_RENT_AM <= quantiles_999['MORTGAGE_PAYMT_OR_RENT_AM']) | (
    df.MORTGAGE_PAYMT_OR_RENT_AM.isNull()))

# Imputing missing cash down amounts
df = df.withColumn('CASH_DOWN_AM', F.coalesce(df.DOWN_PAYMENT, df.CASH_DOWN_AM))

df = df.withColumn('cal_unpaid',
                   F.coalesce(df.CASH_SELL_PRC_AM, F.lit(0)) +
                   F.coalesce(df.SLS_TAX_AM, F.lit(0)) +
                   F.coalesce(df.TITLE_AND_LIC_AM, F.lit(0)) -
                   F.coalesce(df.CASH_DOWN_AM, F.lit(0)) +
                   F.coalesce(df.FRONT_END_FEE_AM, F.lit(0)) -
                   F.coalesce(df.REBATE_AM, F.lit(0)) -
                   F.coalesce(df.TRADE_IN_VAL_AM, F.lit(0)) +
                   F.coalesce(df.OTHER_FINANCE_FEES, F.lit(0)))

df = df.withColumn('UNPAID_BALANCE_AM',
                   F.when((df.UNPAID_BALANCE_AM.isNull()) & (df.cal_unpaid != 0), df.cal_unpaid)
                   .otherwise(df.UNPAID_BALANCE_AM))

df = df.withColumn('cal_EST_AMT_FINANCED',
                   F.coalesce(df.ACCIDENT_HEALTH_INSCE_AM, F.lit(0)) +
                   F.coalesce(df.BACK_END_FEE_AM, F.lit(0)) +
                   F.coalesce(df.CREDIT_LIFE_INSCE_AM, F.lit(0)) +
                   F.coalesce(df.GAP, F.lit(0)) +
                   F.coalesce(df.WARRANTY_AM, F.lit(0)) +
                   F.coalesce(df.UNPAID_BALANCE_AM, F.lit(0)))

df = df.withColumn('EST_AMT_FINANCED',
                   F.when((df.EST_AMT_FINANCED.isNull()) & (df.cal_EST_AMT_FINANCED != 0), df.cal_EST_AMT_FINANCED)
                   .otherwise(df.EST_AMT_FINANCED))

# 1) Rate model specific
# removing missing values for the main variables where imputation cannot work
var_lst = ['TERM', 'LOW_SCORE', 'LOAN_TO_VAL_RT']
df = df.dropna(subset=var_lst)

df = df.filter(df.NEW_USED_CD.isin(['USED', 'CERTIFIED PREOWNED']))

# Using Cash Down and Down payment to create a more accurate cash down amount field
df.withColumn('CASH_DOWN_AM', F.coalesce(df.DOWN_PAYMENT, df.CASH_DOWN_AM))

# Using Amount Financed and Approved loan amount to create a more accurate cash down amount field
df.withColumn('AMT_FINANCED', F.coalesce(df.AMT_FINANCED, df.APRV_LOAN_AMT))

# Removing rows where salary is 0
df = df.filter((df.SALARY_ANNUAL != 0) | (df.SALARY_ANNUAL.isNull()))

# Filtering data only to Auto Loans
df = df.filter((df.LOAN_TYPE == 'AUTO LOAN') | (df.LOAN_TYPE == 'A') | (df.LOAN_TYPE.isNull()))

# Imputing Cash Down from other available fields
df = df.withColumn('CASH_DOWN_AM_CALC',
                   F.round(-(
                       F.coalesce(df.UNPAID_BALANCE_AM, F.lit(0))
                       - F.coalesce(df.CASH_SELL_PRC_AM, F.lit(0))
                       - F.coalesce(df.SLS_TAX_AM, F.lit(0))
                       - F.coalesce(df.TITLE_AND_LIC_AM, F.lit(0))
                       - F.coalesce(df.FRONT_END_FEE_AM, F.lit(0))
                       + F.coalesce(df.REBATE_AM, F.lit(0))
                       + F.coalesce(df.TRADE_IN_VAL_AM, F.lit(0))
                       - F.coalesce(df.OTHER_FINANCE_FEES, F.lit(0))), 1))

df = df.withColumn('CASH_DOWN_AM', F.coalesce(df.CASH_DOWN_AM, df.CASH_DOWN_AM_CALC))

df = df.withColumn('CASH_DOWN_AM', F.when(df.CASH_DOWN_AM < 0, 0.0).otherwise(df.CASH_DOWN_AM))

window_spec = Window.partitionBy("DEAL_ID")

# # 2) add max LTV
df = df.withColumn("max_LOAN_TO_VAL_RT", F.max(df.LOAN_TO_VAL_RT).over(window_spec))

df = df.withColumn("min_LOAN_TO_VAL_RT", F.min(df.LOAN_TO_VAL_RT).over(window_spec))
df = df.withColumn("max_CASH_DOWN_AM", F.max(df.CASH_DOWN_AM).over(window_spec))

# populating vehicle info
df = df.withColumn('vehicle_age', F.year(df.SBMT_TS) - df.AUTO_YEAR) \
    .withColumn('MAKE', F.coalesce(df.MAKE_API, df.AUTO_MAKE)) \
    .withColumn('MODEL', F.coalesce(df.MODEL_API, df.AUTO_MODEL)) \
    .withColumn('TRIM', F.coalesce(df.TRIM_API, df.AUTO_TRIM))

# 1) Rate model specific: vehicle_age & BUY_RATE
# setting list of variables that we would to use for filtering out missing value records
# drop na in vehicle_age in model-spesific dataset
var_lst = [
    'TERM',
    'LOW_SCORE',
    'LOAN_TO_VAL_RT',
    'vehicle_age',
    'SALARY_ANNUAL',
    'CASH_DOWN_AM',
    'EST_AMT_FINANCED',
    'MILEAGE',
    'STATE',
    'LENDER_ID'
]

# filtering out missing value records
df = df.dropna(subset=var_lst)

# Renaming columns
col_mapping = {
    'TERM': 'term',
    'LOW_SCORE': 'lowScore',
    'LOAN_TO_VAL_RT': 'loanToValueRatio',
    'vehicle_age': 'vehicleAge',
    'MAKE': 'make',
    'MODEL': 'model',
    'TRIM': 'trim',
    'SALARY_ANNUAL': 'salaryAnnual',
    'CASH_DOWN_AM': 'cashDownAmount',
    'EST_AMT_FINANCED': 'estimatedAmountFinanced',
    'MILEAGE': 'mileage',
    'STATE': 'state',
    'LENDER_ID': 'lenderIdActual',
    'BUY_RATE': 'buyRate',
    'DEAL_ID': 'dealId',
    'DEALER_ID': 'dealerId',
    'CREDIT_APP_ID': 'creditAppId',
    'SBMT_TS': 'SBMT_TS',
    'APP_DRPTS_DECISION': 'APP_DRPTS_DECISION',
    'APP_CONTRACT_IND': 'APP_CONTRACT_IND',
    'min_LOAN_TO_VAL_RT': 'min_LOAN_TO_VAL_RT',
    'max_CASH_DOWN_AM': 'max_CASH_DOWN_AM',
    'year': 'year',
    'month': 'month'
}


def validate_using_statistic(statics, retailer_name, dataset_type):
    if not statics:
        return None

    warn_messages = []
    for st in statistic_thresholds.get(retailer_name):
        for ran in statistic_thresholds.get(retailer_name).get(st):

            threshold_value = statistic_thresholds.get(retailer_name).get(st).get(ran)
            actual_value = statics.get(st).get(str(ran))
            if threshold_value > 0 and actual_value:
                if abs(actual_value - threshold_value) / threshold_value > statistic_relative_errors.get(st) \
                    or actual_value == 0:
                    warn_messages.append(f'Deviation of {st} for range {ran} '
                                         f'{(abs(actual_value - threshold_value) / threshold_value):.2%}: '
                                         f'current {actual_value}, threshold {threshold_value}')

    if warn_messages:
        LOG.warn(f'{job_name}: {WARN_MESSAGE_PREFIX} '
                 f'Validation warnings for dataset {retailer_name} {dataset_type}".\n' + '\n'.join(warn_messages))


bucketizer = Bucketizer()


def calculate_statistics(dataframe, retailer_name: str):
    statistics = {}
    rows_count = dataframe.count()

    if rows_count == 0:
        return None

    for metric in statistic_thresholds.get(retailer_name):
        interval = list(statistic_thresholds.get(retailer_name).get(metric))
        if isinstance(interval[0], tuple):
            split_threshold = sorted(list(set([j for i in interval for j in i])))
            splits = [-float("inf"), *split_threshold, float("inf")]

            bucketizer.setSplits(splits)
            bucketizer.setInputCol(metric)
            bucketizer.setOutputCol("buckets")
            bucketed = bucketizer.setHandleInvalid("keep").transform(dataframe)
            bucketed = bucketed.withColumn("buckets", F.col("buckets").cast(StringType()))

            splits_dict = {str(float(i)): f'({splits[i]}, {splits[i + 1]})' for i in range(len(splits) - 1)}
            bucketed = bucketed.groupBy("buckets").count()
            bucketed = bucketed.replace(to_replace=splits_dict, subset=['buckets'])

            bucketed = bucketed.filter((~F.col('buckets').contains('inf')))
        else:
            if metric != "total_count":
                bucketed = dataframe.groupBy(metric) \
                    .count() \
                    .filter(F.col(metric).isin(interval))
            else:
                bucketed = spark.createDataFrame(
                    [
                        ("total_count", dataframe.count())
                    ],
                    ["total_count", "value"]
                )

        distributions = {}

        for row in bucketed.collect():
            range_item = row[0]
            count_values_in_range = row[1]
            distribution = round(count_values_in_range / rows_count * 100, 3)
            distributions.update({str(range_item): distribution})
            statistics.update({metric: distributions})

    statistics.update({"total_count": {"*": rows_count}})

    return statistics


def dataset_validation(result_df, folder_name):
    # calculate statistic for DF
    start = dt.date.today() - dt.timedelta(days=14)
    delta = result_df.filter(result_df.SBMT_TS >= start)

    # Delta validation
    stats = calculate_statistics(delta, f'{folder_name}')
    validate_using_statistic(stats, folder_name, "delta")

    # Complete validation
    stats = calculate_statistics(result_df, f'{folder_name}')

    if stats:
        s3_client.put_object(Bucket=f"{data_science_bucket_name}",
                             Key=f"{data_science_folder}/fni_rate_estimation/{folder_name}/statistics.json",
                             Body=json.dumps(stats).encode('utf-8'))

    validate_using_statistic(stats, folder_name, "complete")


# storing the final data in S3
def write_result(result_df, folder_name):
    if result_df.rdd.isEmpty():
        print(f"Processing result for {folder_name} is empty. Training dataset can't be generated.")

    # dataset_validation(result_df, folder_name)

    # approximate row size in bytes
    row_size = 100
    # desired max partition size in bytes
    max_partition_size = 50 * 1000 * 1000 * 1000

    if result_df.count() * row_size <= max_partition_size:
        # data fits into one partition
        df_w = result_df.coalesce(1)
    else:
        # there is a possibility that data doesn't fit into one partition
        df_w = result_df \
            .repartition('year', 'month')

    df_w.drop('year').drop('month').write \
        .format('parquet') \
        .mode('overwrite') \
        .save(f's3://{data_science_bucket_name}/awsdri/pv_tests/basic_training/ltv_sample1')


df = df.persist(StorageLevel.MEMORY_AND_DISK)

# complete dataset
complete_df = df.cache()

print("complete df before mapping")
print(complete_df.count())
complete_df.printSchema()

for old_name, new_name in col_mapping.items():
    complete_df = complete_df.withColumnRenamed(old_name, new_name)

complete_df = complete_df.cache()

print("complete df after mapping")
print(complete_df.count())
complete_df.printSchema()

write_result(complete_df, 'total')