import pandas as pd
import numpy as np
import requests
import os
from dotenv import load_dotenv
import fmpsdk as fmp
from pyspark.sql import SparkSession

load_dotenv()
fmp_key = os.getenv('fmp_key')

try :
    spark = SparkSession.builder.appName("cryptoData") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .getOrCreate()
    print("Connection Successful")
except:
    print("Could not bind spark to a port!.")
try:
    btc_df = fmp.historical_price_full(apikey=fmp_key,symbol="BTCUSD",from_date='2004-02-10')
    df_pandas = pd.DataFrame(btc_df)
    spark_df = spark.createDataFrame(df_pandas)
    print("Data extracted for API suucessfully.")
except:
    print("Data extraction failed from the API.")

try:
    spark_df.write \
        .format("jdbc") \
        .option("url","jdbc:sqlserver://ZAHRA\SQLEXPRESS:61254;database=crypto_data;trustServerCertificate=true;encrypt=true") \
        .option("dbtable","btc_data") \
        .option("user","mehassan") \
        .option("password","password") \
        .save()
    print("Database populated with the crypto data.")
except:
    print("Could not populate database.")

spark.stop()