from pyspark.sql.types import *
import yaml 
import requests
import pyspark.sql.functions as F

def get_config(filename):
    with open(filename, 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

def getSchemaDict(config: dict,tablename: str):
    return config['inputTables'][tablename]['SCHEMA']

def getOutputCols(config: dict,tablename: str):
    return config['outputTables'][tablename]['COLUMNS'].split(' ')

def getOutputDictNullable(config: dict,tablename: str):
    return config['outputTables'][tablename]['NULLABLE']

def getPrimaryKeys(config: dict,tablename: str):
    return config['PRIMARY_KEYS'][tablename].split(' ')

def getTimeFormat(config: dict,tablename: str):
    return config['TIME_FORMAT'][tablename]

def getCsvOptions(config: dict,tablename: str):
    return config['configurationCsv'][tablename]

def get_spark_datatype(str):
    dict_spark_datatype = {
        "STRING": StringType(),
        "INTEGER": IntegerType(),
        "DECIMAL(16,5)": DecimalType(16,5),
        "DATE": DateType(),
        "TIMESTAMP": TimestampType()
    }
    return dict_spark_datatype[str]

def InitSchema(schema_dict):
    schema = StructType([])
    for k in schema_dict.keys():
        field_name = k
        field_datatype = get_spark_datatype(schema_dict[k])
        schema.add(field_name, field_datatype, True)
    return schema

def get_NSE_ID(claim_id):
    url = "https://api.hashify.net/hash/md4/hex"
    data = {"value": claim_id}
    req_json = requests.post(url=url,json=data).json()
    digest = req_json['Digest']
    return digest 
get_NSE_ID_udf = F.udf(lambda x:get_NSE_ID(x))