from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType
)

part_schema = StructType([
    StructField('user_login_id', StringType(), True),
    StructField('report_id', StringType(), True),
    StructField('currency_code', StringType(), True),
    StructField('submit_date', DateType(), True),
    StructField('orgunit4', StringType(), True),
    StructField('ingestion_time', StringType(), True),
    StructField('creation_date', TimestampType(), True),
    StructField('employee_id', StringType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('report_entry_id', StringType(), True),
    StructField('expense_type_name', StringType(), True),
    StructField('expense_type_id', StringType(), True),
    StructField('business_purpose', StringType(), True),
    StructField('country', StringType(), True),
    StructField('vendor_description', StringType(), True),
    StructField('transaction_currency_code', StringType(), True),
    StructField('location_subdivision', StringType(), True),
    StructField('spend_category', StringType(), True),
    StructField('itemization_id', StringType(), True),
    StructField('transaction_amount_lc', DecimalType(), True),
    StructField('transaction_date', DateType(), True),
    StructField('company_code', StringType(), True),
    StructField('allocation_id', StringType(), True),
    StructField('type', StringType(), True),
    StructField('cost_center_id', StringType(), True),
    StructField('order_number', StringType(), True),
    StructField('wbs_element', StringType(), True),
    StructField('cost_center_text', StringType(), True),
    StructField('transaction_amount_usd', DecimalType(), True),
    StructField('creation_year', IntegerType(), True),
    StructField('creation_month', IntegerType(), True),
    StructField('creation_day', IntegerType(), True),
])
