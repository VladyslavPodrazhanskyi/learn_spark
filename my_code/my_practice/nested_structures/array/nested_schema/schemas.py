from pyspark.sql.types import (
	StructType,  StructField,  LongType, DoubleType, ArrayType,
	DecimalType,  StringType,  TimestampType
)

#  user_login_id string
#  report_id string
#  currency_code string
#  submit_date timestamp
#  orgunit4 string
#  ingestion_time string
#  report_owner:
	# 	long,  string,  long*4,  string*6
#  expense_entries_list:
	# 	string,  decimal,  decimal,  string,
	# 	cardtransaction:
	# 		string,  long,  string * 3,  double,  long,  string * 5,  decimal,
	# 		timestamp,  (decimal,  string)*2,  timestamp,  string * 3
    # 	long
	# 	custom1: type,  value
	# 	custom10: my_code,  type,  value
	# 	custom2: my_code,  type,  value
	# 	custom3: type,  value
	# 	custom35: type,  value
	# 	custom5: type,  value
	#   string * 2,  double,  string*14
	#   itemizationslist:
	#   	itemization:
	#			allocationslist:
	# 				allocation:
	#   				long,  long,  string
	#   				custom1: my_code,  type,  value (string*3)
	#   				custom2: my_code,  type,  value (string*3)
	#   				custom3: my_code,  type,  value (string*3)
	#   				custom4: my_code,  type,  value (string*3)
	#   				custom7: my_code,  type,  value (string*3)
	#   				custom8: my_code,  type,  value (string*3)
	#   				journalentrieslist:
	#   					journalentry: long,  decimal,  string,  long*2,  string*4
	# 					string * 2 ( percentage,  vatdatalist)
	#			approvedamount - decimal
	#			attendeeslist:
	#				attendee:
	#					string*6
	#					custom2: type,  value
	#					custom3: type,  value
	#					custom4: type,  value
	#					custom5: type,  value
	#					custom6: type,  value
	#					string*8,  decimal*2,  long
	#			businesspurpose,  string
	#			commentcount,  long
	#			custom1: type,  value (string*2)
	#			custom10: my_code,  type,  value (string*3)
	#			custom2: my_code,  type,  value (string*3)
	#			custom3: type,  value (string*2)
	#			custom35: type,  value (string*2)
	#			custom5: type,  value (string*2)
	#			string*6, timestamp,  string*6,  decimal*2,  timestamp
	#   lastmodified, timestamp
	#   string*5,  decimal,  string*6,  decimal,  string*2,  timestamp,  string*2


part_schema = StructType([
	StructField('user_login_id',  StringType(), True),
	StructField('report_id', StringType(), True),
	StructField('currency_code', StringType(), True),
	StructField('submit_date', TimestampType(), True),
	StructField('orgunit4', StringType(), True),
	StructField('ingestion_time', StringType(), True),
	StructField('report_owner', StructType([
			StructField('employeecustom21', LongType(), True),
			StructField('employeeid', StringType(), True),
			StructField('employeeorgunit1', LongType(), True),
			StructField('employeeorgunit2', LongType(), True),
			StructField('employeeorgunit3', LongType(), True),
			StructField('employeeorgunit4', LongType(), True),
			StructField('employeeorgunit5', StringType(), True),
			StructField('employeeorgunit6', StringType(), True),
			StructField('firstname', StringType(), True),
			StructField('lastname', StringType(), True),
			StructField('middleinitial', StringType(), True),
			StructField('reimbursementmethodcode', StringType(), True)
		]), True),
])


full_schema = StructType([
	StructField('user_login_id',  StringType(), True),
	StructField('report_id', StringType(), True),
	StructField('currency_code', StringType(), True),
	StructField('submit_date', TimestampType(), True),
	StructField('orgunit4', StringType(), True),
	StructField('ingestion_time', StringType(), True),
	StructField('report_owner', StructType([
		StructField('employeecustom21', LongType(), True),
		StructField('employeeid', StringType(), True),
		StructField('employeeorgunit1', LongType(), True),
		StructField('employeeorgunit2', LongType(), True),
		StructField('employeeorgunit3', LongType(), True),
		StructField('employeeorgunit4', LongType(), True),
		StructField('employeeorgunit5', StringType(), True),
		StructField('employeeorgunit6', StringType(), True),
		StructField('firstname', StringType(), True),
		StructField('lastname', StringType(), True),
		StructField('middleinitial', StringType(), True),
		StructField('reimbursementmethodcode', StringType(), True)
	]), True),
	StructField('expense_entries_list', ArrayType(StructType([
		StructField('allocation_url', StringType(), True),
		StructField('approvedamount', DecimalType(20, 2), True),
		StructField('billingamount', DecimalType(20, 2), True),
		StructField('businesspurpose', StringType(), True),
		StructField('cardtransaction', StructType([
			StructField('accountnumber', StringType(), True),
			StructField('accountnumberlastsegment', LongType(), True),
			StructField('carddescription', StringType(), True),
			StructField('cardtypecode', StringType(), True),
			StructField('description', StringType(), True),
			StructField('exchangeratefrombillingtoemployeecurrency', DoubleType(), True),
			StructField('mastercardcode', LongType(), True),
			StructField('merchantcity', StringType(), True),
			StructField('merchantcountrycode', StringType(), True),
			StructField('merchantreferencenumber', StringType(), True),
			StructField('merchantstate', StringType(), True),
			StructField('postedalphacode', StringType(), True),
			StructField('postedamount', DecimalType(20, 2), True),
			StructField('posteddate', TimestampType(), True),
			StructField('taxamount', DecimalType(20, 2), True),
			StructField('transactionalphacode', StringType(), True),
			StructField('transactionamount', DecimalType(20, 2), True),
			StructField('transactionccttype', StringType(), True),
			StructField('transactiondate', TimestampType(), True),
			StructField('transactionid', StringType(), True),
			StructField('transactionmerchantname', StringType(), True),
			StructField('transactionreferencenumber', StringType(), True)
			]), True),
		StructField('commentcount', LongType(), True),
		StructField('custom1', StructType([
			StructField('type', StringType(), True),
			StructField('value', StringType(), True)
		]), True),
		StructField('custom10', StructType([
			StructField('my_code', StringType(), True),
			StructField('type', StringType(), True),
			StructField('value', StringType(), True)
		]), True),
		StructField('custom2', StructType([
			StructField('my_code', StringType(), True),
			StructField('type', StringType(), True),
			StructField('value', StringType(), True)
		]), True),
		StructField('custom3', StructType([
			StructField('type', StringType(), True),
			StructField('value', LongType(), True)
		]), True),
		StructField('custom35', StructType([
			StructField('type', StringType(), True),
			StructField('value', StringType(), True)
		]), True),
		StructField('custom5', StructType([
			StructField('type', StringType(), True),
			StructField('value', StringType(), True)
		]), True),
		StructField('e_receiptid', StringType(), True),
		StructField('entryimageid', StringType(), True),
		StructField('exchangerate', DoubleType(), True),
		StructField('expensepay', StringType(), True),
		StructField('expensetypeid', StringType(), True),
		StructField('expensetypename', StringType(), True),
		StructField('formid', StringType(), True),
		StructField('hasallocation', StringType(), True),
		StructField('hasattendees', StringType(), True),
		StructField('hascomments', StringType(), True),
		StructField('hasexceptions', StringType(), True),
		StructField('hasvat', StringType(), True),
		StructField('imagerequired', StringType(), True),
		StructField('iscreditcardcharge', StringType(), True),
		StructField('isitemized', StringType(), True),
		StructField('ispersonal', StringType(), True),
		StructField('ispersonalcardcharge', StringType(), True),
		StructField('itemizationslist', StructType([
			StructField('itemization', ArrayType(StructType([
				StructField('allocationslist', StructType([
					StructField('allocation', ArrayType(StructType([
						StructField('accountcode1', LongType(), True),
						StructField('accountcode2', LongType(), True),
						StructField('allocationid', StringType(), True),
						StructField('custom1', StructType([
							StructField('my_code', StringType(), True),
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom2', StructType([
							StructField('my_code', StringType(), True),
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom3', StructType([
							StructField('my_code', StringType(), True),
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom4', StructType([
							StructField('my_code', StringType(), True),
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom7', StructType([
							StructField('my_code', StringType(), True),
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom8', StructType([
							StructField('my_code', StringType(), True),
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('journalentrieslist', StructType([
							StructField('journalentry', ArrayType(
								StructType([
									StructField('accountcode', LongType(), True),
									StructField('amount', DecimalType(20, 2), True),
									StructField('debitorcredit', StringType(), True),
									StructField('jobrunkey', LongType(), True),
									StructField('journalid', LongType(), True),
									StructField('payeepaymenttypecode', StringType(), True),
									StructField('payeepaymenttypename', StringType(), True),
									StructField('payerpaymenttypecode', StringType(), True),
									StructField('payerpaymenttypename', StringType(), True)
								]),
							True), True)
						]), True),
						StructField('percentage', StringType(), True),
						StructField('vatdatalist', StringType(), True)
					]), True), True)
				]), True),                         # finish allocationslist
				StructField('approvedamount', DecimalType(20, 2), True),
				StructField('attendeeslist', StructType([
					StructField('attendee', ArrayType(StructType([
						StructField('attendeeid', StringType(), True),
						StructField('attendeeownerid', StringType(), True),
						StructField('attendeetype', StringType(), True),
						StructField('attendeetypecode', StringType(), True),
						StructField('company', StringType(), True),
						StructField('currencycode', StringType(), True),
						StructField('custom2', StructType([
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom3', StructType([
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom4', StructType([
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom5', StructType([
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('custom6', StructType([
							StructField('type', StringType(), True),
							StructField('value', StringType(), True)
						]), True),
						StructField('externalid', StringType(), True),
						StructField('firstname', StringType(), True),
						StructField('hasexceptionsprevyear', StringType(), True),
						StructField('hasexceptionsytd', StringType(), True),
						StructField('isdeleted', StringType(), True),
						StructField('lastname', StringType(), True),
						StructField('ownerempname', StringType(), True),
						StructField('title', StringType(), True),
						StructField('totalamountprevyear', DecimalType(20, 2), True),
						StructField('totalamountytd', DecimalType(20, 2), True),
						StructField('versionnumber', LongType(), True)
					]),  True), True)
				]), True),
				StructField('businesspurpose', StringType(), True),
				StructField('commentcount', LongType(), True),
				StructField('custom1', StructType([
					StructField('type', StringType(), True),
					StructField('value', StringType(), True)
				]), True),
				StructField('custom10', StructType([
					StructField('my_code', StringType(), True),
					StructField('type', StringType(), True),
					StructField('value', StringType(), True)
				]), True),
				StructField('custom2', StructType([
					StructField('my_code', StringType(), True),
					StructField('type', StringType(), True),
					StructField('value', StringType(), True)
				]), True),
				StructField('custom3', StructType([
					StructField('type', StringType(), True),
					StructField('value', LongType(), True)
				]), True),
				StructField('custom35', StructType([
					StructField('type', StringType(), True),
					StructField('value', StringType(), True)
				]), True),
				StructField('custom5', StructType([
					StructField('type', StringType(), True),
					StructField('value', StringType(), True)
				]), True),
				StructField('expensetypeid', StringType(), True),
				StructField('expensetypename', StringType(), True),
				StructField('hascomments', StringType(), True),
				StructField('ispersonal', StringType(), True),
				StructField('itemtype', StringType(), True),
				StructField('itemizationid', StringType(), True),
				StructField('lastmodified', TimestampType(), True),
				StructField('orgunit1', StringType(), True),
				StructField('orgunit2', StringType(), True),
				StructField('orgunit3', StringType(), True),
				StructField('orgunit4', StringType(), True),
				StructField('orgunit5', StringType(), True),
				StructField('orgunit6', StringType(), True),
				StructField('postedamount', DecimalType(20, 2), True),
				StructField('transactionamount', DecimalType(20, 2), True),
				StructField('transactiondate', TimestampType(), True)
			]), True), True) # itemisation finished
		]), True),           #  itemisationlist finished
		StructField('lastmodified', TimestampType(), True),
		StructField('locationcountry', StringType(), True),
		StructField('locationname', StringType(), True),
		StructField('locationsubdivision', StringType(), True),
		StructField('paymenttypecode', StringType(), True),
		StructField('paymenttypename', StringType(), True),
		StructField('postedamount', DecimalType(20, 2), True),
		StructField('receiptrequired', StringType(), True),
		StructField('reportentryid', StringType(), True),
		StructField('reportentryreceiptreceived', StringType(), True),
		StructField('reportentryreceipttype', StringType(), True),
		StructField('reportentryvendorname', StringType(), True),
		StructField('spendcategory', StringType(), True),
		StructField('transactionamount', DecimalType(20, 2), True),
		StructField('transactioncurrencycode', StringType(), True),
		StructField('transactioncurrencyname', StringType(), True),
		StructField('transactiondate', TimestampType(), True),
		StructField('userloginid', StringType(), True),
		StructField('vendordescription', StringType(), True)
	]),  True), True)
])