from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import utilities as utils
import os 

FILENAME = 'config.yml'
REFERENCE_PATH = os.getcwd()

# Create a SparkSession object
spark = SparkSession.builder \
    .appName("fincons") \
    .master("local") \
    .getOrCreate()

# Read configuration
config = utils.get_config(FILENAME)

# Input Tables Schema
schema_contract = utils.getSchemaDict(config,'CONTRACT')
schema_claim = utils.getSchemaDict(config,'CLAIM')

# Output Tables Columns
output_cols = utils.getOutputCols(config,'TRANSACTIONS')
output_dict_nullable = utils.getOutputDictNullable(config,'TRANSACTIONS')

# Get Primary Keys
pk_contract = utils.getPrimaryKeys(config,'CONTRACT')
pk_claim = utils.getPrimaryKeys(config,'CLAIM')
pk_transaction = utils.getPrimaryKeys(config,'TRANSACTIONS')

# Csv Configuration
csv_options_contract = utils.getCsvOptions(config,'CONTRACT')
csv_options_claim = utils.getCsvOptions(config,'CLAIM')
csv_options_transactions = utils.getCsvOptions(config,'TRANSACTIONS')

# Input Mapping Column (to handle wrong column names in input files)
# mapping_column = config['mappingColumns']

# Date and Timestamp Field Formats
time_formats_contract = utils.getTimeFormat(config,'CONTRACT')
time_formats_claim = utils.getTimeFormat(config,'CLAIM')
time_formats_output = utils.getTimeFormat(config,'TRANSACTIONS')

# Init Dataframes
df_contract = spark.createDataFrame(data=[],schema=utils.InitSchema(schema_contract))
df_claim = spark.createDataFrame(data=[],schema=utils.InitSchema(schema_claim))

# Read Tables
df_contract_file = spark.read.options(**csv_options_contract).csv(os.path.join(REFERENCE_PATH,'contract.csv'))
df_claim_file = spark.read.options(**csv_options_claim).csv(os.path.join(REFERENCE_PATH,'claim.csv'))

# Get Dataframes - input schema is "forced" to match input schema requirements
df_contract = df_contract.union(df_contract_file)
df_claim = df_claim.union(df_claim_file)

# Map Column
# for m in mapping_column.keys():
#     if m in df_contract.columns:
#         df_contract = df_contract.withColumnRenamed(m, mapping_column[m])
#     if m in df_claim.columns:
#         df_claim = df_claim.withColumnRenamed(m, mapping_column[m])

# Cast Files
cast_expression_contract = []
cast_expression_claim = []

for f in schema_contract.keys():
    datatype = schema_contract[f]
    if datatype in ('DATE','TIMESTAMP') and f in time_formats_contract:
        cast_expression_contract.append(f"to_timestamp({f},'{time_formats_contract[f]}') as {f}")
    else:
        cast_expression_contract.append(f"cast({f} as {datatype})")
df_contract = df_contract.selectExpr(cast_expression_contract)

for f in schema_claim.keys():
    datatype = schema_claim[f]
    if datatype in ('DATE','TIMESTAMP') and f in time_formats_claim:
        cast_expression_claim.append(f"to_timestamp({f},'{time_formats_claim[f]}') as {f}")
    else:
        cast_expression_claim.append(f"cast({f} as {datatype})")
df_claim = df_claim.selectExpr(cast_expression_claim)

# Set Primary Keys
for k in pk_contract:
    df_contract = df_contract.withMetadata(k, {'primary_key': True})
for k in pk_claim:
    df_claim = df_claim.withMetadata(k, {'primary_key': True})

# Mapping to create table Transactions
df_transactions = df_contract.drop('CREATION_DATE').join(df_claim, on = 'CONTRACT_ID', how='inner')
df_transactions = df_transactions.withColumn('CONTRACT_SOURCE_SYSTEM', F.lit('Europe 3')) \
    .withColumnRenamed("AMOUNT","CONFORMED_VALUE") \
    .withColumn("CONTRACT_SOURCE_SYSTEM_ID",F.col("CONTRACT_ID").cast("long")) \
    .withColumnRenamed("DATE_OF_LOSS","BUSINESS_DATE") \
    .withColumn('SOURCE_SYSTEM_ID',F.element_at(F.split(F.col('CLAIM_ID'),'_'),2).cast("integer")) \
    .withColumn('TRANSACTION_TYPE',F.when(F.col('CLAIM_TYPE') == 2, "Corporate")
                                    .when(F.col('CLAIM_TYPE') == 1, "Private")
                                    .when(F.col('CLAIM_TYPE').isNull(), "Unknown")
                                    .otherwise(None)) \
    .withColumn('TRANSACTION_DIRECTION',F.when(F.col('CLAIM_ID').startswith('CL_'), "COINSURANCE")
                                    .when(F.col('CLAIM_ID').startswith('RX_'), "REINSURANCE")
                                    .otherwise(None)) \
    .withColumn('SYSTEM_TIMESTAMP',F.current_timestamp()) \
    .withColumn('NSE_ID',utils.get_NSE_ID_udf(F.col('CLAIM_ID')))

for c in time_formats_output.keys():
    if c in df_transactions.columns:
        if c in 'BUSINESS_DATE':
            df_transactions = df_transactions.withColumn(c,F.date_format(F.col(c),time_formats_output[c]).cast('date'))
        else:
            df_transactions = df_transactions.withColumn(c,F.date_format(F.col(c),time_formats_output[c]).cast('timestamp'))

df_transactions = df_transactions.select(*output_cols)

# Set Primary Keys
for k in pk_transaction:
    df_transactions = df_transactions.withMetadata(k, {'primary_key': True})

# Set Columns Nullable
for k in output_dict_nullable.keys():
    df_transactions.schema[k].nullable = bool(output_dict_nullable[k])

# df_transactions.write.format('csv').mode('overwrite').options(**csv_options_transactions).csv(os.path.join(REFERENCE_PATH,'transactions.csv'))
df_transactions.toPandas().to_csv(os.path.join(REFERENCE_PATH,'transactions.csv'), header=csv_options_transactions['header'], sep=csv_options_transactions['delimiter'])

spark.stop()