from __future__ import annotations

from datetime import date

import fmpsdk as fmp
import pandas as pd

from src.common.decorator import ApiDecorator


class FmpData:
    """
    To reuse all these methods, it's better to put them into one class
    """

    def __init__(self, api_key: str = '',
                 mssql_conf: dict = None,
                 maria_conf: dict = None,
                 mongo_conf: dict = None):
        if not api_key:
            raise ValueError('Ops, please initialize with FMP Api Key')
        self.api_key = api_key
        self.__table_name = ''
        self.mssql_conf = mssql_conf
        self.maria_conf = maria_conf
        self.mongo_conf = mongo_conf

    @property
    def mongo_uri(self):
        return self.__generate_mongo_uri()

    @property
    def mssql_jdbc(self):
        return self.__generate_mssql_jdbc()

    @property
    def mssql_property(self):
        return self.__generate_mssql_property()

    @property
    def maria_jdbc(self):
        return self.__generate_maria_jdbc()

    @property
    def maria_property(self):
        return self.__generate_maria_property()

    def __generate_mongo_uri(self) -> str | None:
        """
        generate mongo connection uri based on input
        :return:
        """
        if not self.mongo_conf:
            return None
        return (f"mongodb+srv://{self.mongo_conf['user']}:"
                f"{self.mongo_conf['token']}@"
                f"{self.mongo_conf['host']}"
                f"/?retryWrites=true&w=majority")

    def __generate_mssql_jdbc(self) -> str | None:
        if not self.mssql_conf:
            return None
        return (f"jdbc:sqlserver://{self.mssql_conf['host']}:{self.mssql_conf['port']};"
                f"databaseName={self.mssql_conf['database']};encrypt=true;")

    def __generate_mssql_property(self) -> dict:
        if not self.mongo_conf:
            return {}
        return {key: self.mssql_conf[key] for key in self.mssql_conf.keys()
                & {'user', 'password', 'driver'}}

    def __generate_maria_jdbc(self) -> str | None:
        if not self.mssql_conf:
            return None
        return (f"jdbc:sqlserver://{self.maria_conf['host']}:{self.maria_conf['port']};"
                f"databaseName={self.maria_conf['database']};encrypt=true;")

    def __generate_maria_property(self) -> dict:
        if not self.mongo_conf:
            return {}
        return {key: self.maria_conf[key] for key in self.maria_conf.keys()
                & {'user', 'password', 'driver'}}

    @property
    def table_name(self):
        return self.__table_name

    @table_name.setter
    def table_name(self, value):
        self.__table_name = value

    # @ApiDecorator.write_to_mssql_sp
    @ApiDecorator.write_to_maria_sp
    def get_fmp_cot(self, commodity_name: str = '',
                    start_date: str = '',
                    to_date: str = '') -> pd.DataFrame:
        """
        This function returns information on the current positions
        held by commercial and non-commercial hedgers. It also returns the spread between their long,
        and short positions

        NOTE: Need to set self.table_name before using this method

        :param commodity_name: commodity_name
        :param start_date: start_date
        :param to_date: to_date
        :return: pd dataframe
        """
        to_date = to_date if to_date else date.today().strftime('%Y-%m-%d')
        api_data = fmp.commitment_of_traders_report(self.api_key, commodity_name, from_date=start_date,
                                                    to_date=to_date)
        df = pd.DataFrame(api_data)
        df.date = pd.to_datetime(df.date)

        cot = df[['date', 'noncomm_positions_long_all', 'noncomm_positions_short_all', 'comm_positions_long_all',
                  'comm_positions_short_all']]
        cot['comm_spread'] = cot['comm_positions_long_all'] - cot['comm_positions_short_all']
        cot['non_comm_spread'] = cot['noncomm_positions_long_all'] - cot['noncomm_positions_short_all']

        return cot
