import functools
import json

import pandas as pd
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from src.common.spark import MySpark


class ApiDecorator:

    @classmethod
    def __prepare_dataframe(cls, spark, sc, data) -> DataFrame:
        """
        prepare dataframe when the data might be in different data types
        :param spark: spark session
        :param sc: spark context
        :param data: data to be processed
        :return: spark sql dataframe
        """
        if isinstance(data, pd.DataFrame) and not data.empty:
            df = spark.createDataFrame(data)
        elif isinstance(data, list):
            df = spark.read.json(sc.parallelize([json.dumps(record) for record in data]))
        elif isinstance(data, dict):
            data = [data]
            df = spark.read.json(sc.parallelize([json.dumps(record) for record in data]))
        else:
            # initialize empty dataframe
            schema = StructType([])
            df = spark.createDataFrame([], schema)

        return df

    @classmethod
    def write_to_mssql_sp(cls, func):
        """
        decorator that write the result data into mssql
        :param func:
        :return:
        """
        @functools.wraps(func)
        def _call_wrapper(self, *args, **kwargs):
            response = func(self, *args, **kwargs)
            spark = MySpark.initialize_spark(mongo_uri=self.mongo_uri)
            sc = spark.sparkContext
            df = ApiDecorator.__prepare_dataframe(spark, sc, response)
            if not df.isEmpty():
                try:
                    df.write.jdbc(
                        url=self.mssql_jdbc,
                        table=self.table_name,
                        mode="append",
                        properties=self.mssql_property
                    )
                except PySparkException as pe:
                    print('error writing in pyspark', pe)
            spark.stop()
            return response

        return _call_wrapper

    @classmethod
    def write_to_maria_sp(cls, func):
        """
        decorator that write the result data into mssql
        :param func:
        :return:
        """

        @functools.wraps(func)
        def _call_wrapper(self, *args, **kwargs):
            response = func(self, *args, **kwargs)
            spark = MySpark.initialize_spark(mongo_uri=self.mongo_uri)
            sc = spark.sparkContext
            df = ApiDecorator.__prepare_dataframe(spark, sc, response)
            print(self.maria_jdbc)
            print(self.table_name)
            print(self.maria_property)
            if not df.isEmpty():
                try:
                    df.write.jdbc(
                        url=self.maria_jdbc,
                        table=self.table_name,
                        mode="append",
                        properties=self.maria_property
                    )
                except PySparkException as pe:
                    print('error writing in pyspark', pe)
            spark.stop()
            return response

        return _call_wrapper
