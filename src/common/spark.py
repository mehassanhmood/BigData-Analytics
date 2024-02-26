from __future__ import annotations

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException


class MySpark:
    """
    Initialize the spark
    """

    @staticmethod
    def initialize_spark(name: str = 'Default Project', mongo_uri: str = '') -> SparkSession | None:
        """
        initialize spark
        :param mongo_uri: str, mongo connection uri
        :param name: str, spark application name
        :return:
        """
        conf = SparkConf()
        conf.setAppName(name)
        conf.set("spark.driver.bindAddress", "0.0.0.0")
        if mongo_uri:
            conf.set("spark.mongodb.input.uri", mongo_uri)
            conf.set("spark.mongodb.output.uri", mongo_uri)
            conf.set("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0,"
                                            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        else:
            conf.set("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0")
        try:
            # getting the spark instance
            spark = SparkSession.builder \
                .config(conf=conf) \
                .getOrCreate()
        except PySparkException as e:
            print(f"Failed to get or create Spark: {e}")

            return None
        else:
            return spark
