import fmpsdk as fmp
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import datetime
from pyspark.sql import SparkSession

load_dotenv()
fmp_key = os.getenv('fmp_key')

try :
    spark = SparkSession.builder.appName("cotReport") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .getOrCreate()
    print("Connection Successful")
except:
    print("Could not bind spark to a port!.")

def cot_fmp(commodity_name,start_date = '2004-01-31'):
    """
    This function returns information on the current positions
    held by commercial and non-commercial hedgers. It also returns the spread between their long,
    and short positions
    """

    df = pd.DataFrame(fmp.commitment_of_traders_report(fmp_key,commodity_name,from_date=start_date,to_date=datetime.datetime.now()))
    df.date = pd.to_datetime(df.date)

    cot = df[['date','noncomm_positions_long_all','noncomm_positions_short_all','comm_positions_long_all','comm_positions_short_all']]
    cot['comm_spread'] = cot['comm_positions_long_all']-cot['comm_positions_short_all']
    cot['non_comm_spred'] = cot['noncomm_positions_long_all']-cot['noncomm_positions_short_all']

    return cot

sec = ["BT","ES", "GC", "ZN", "SI", "LS", "NG", "CL", "DX"]
try:
    data = {}
    for i in sec:
        cot = cot_fmp(i)
        data[i] = cot
    print("Data Fetched")
    print("Populating the database.")
except:
    print("Could not Fetch data from the API.")

try:
    for i in sec:
        df = spark.createDataFrame(data[i])
        df.write \
        .format("jdbc") \
        .option("url","jdbc:sqlserver://ZAHRA\SQLEXPRESS:61254;database=cot_report;trustServerCertificate=true;encrypt=true") \
        .option("dbtable",f"{i}") \
        .option("user","mehassan") \
        .option("password","password") \
        .save()
    print("Database populated!")
except:
    print("Could not populate tehb database.")

spark.stop()