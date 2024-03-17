import pandas as pd
import numpy as np
import requests
import yaml
from dotenv import load_dotenv
import fmpsdk as fmp
from pyspark.sql import SparkSession

load_dotenv()
fmp_key = os.getenv('fmp_key')


with open("../conf.yaml", "r") as conf:
    config = yaml.safe_load(conf)

host = config["SqlExpressServer"]["host"]
user = config["SqlExpressServer"]["user"]
password = config["SqlExpressServer"]["password"]
port = config["SqlExpressServer"]["port"]
database = config["SqlExpressServer"]["database_crypto"]
driver = config["SqlExpressServer"]["driver"]
url = f"jdbc:sqlserver://{host}:{port};database={database};trustServerCertificate=true;encrypt=true"


def crypto_data():
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
            .option("url",url) \
            .option("dbtable","btc_data") \
            .option("user",user) \
            .option("password",password) \
            .save()
        print("Database populated with the crypto data.")
    except:
        print("Could not populate database.")

    spark.stop()