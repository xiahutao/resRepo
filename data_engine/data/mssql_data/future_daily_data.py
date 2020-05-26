# -*- coding: utf-8 -*-

import logging
import pyodbc
import pandas as pd

from data_engine.data.future_data import FutureData
from data_engine.setting import FREQ_1D

import data_engine.setting as Setting

_logger = logging.getLogger(__name__)

class FutureDailyData(FutureData):
    '''
        日线数据接口，从MSSQL加载
    '''
    def __init__(self,**kwargs):
        FutureData.__init__(self,freq=FREQ_1D,**kwargs)

    def _read_data_DB(self, query=None):

        try:
            conn = pyodbc.connect(Setting.DB_CONFIG_DAILY)
        except:
            self._logger.error("DB connection error: read, " + Setting.DB_CONFIG_DAILY)
        df = None
        try:
            df = pd.read_sql(query, conn)
        except:
            _logger.error("read failed:", query)
        return df

    def _get_continous_prices(self, cmd, st='2018-01-01', ed='2030-01-01', adjust_method='VOLUME'):

        equity_map = {'IF': '000300', 'IH': '000016', 'IC': '000905'}
        if cmd in equity_map.keys():
            cmd = equity_map[cmd]
        query = ""
        query = """
            SELECT * FROM MARKET.dbo.GET_FUTURES_CONTINUOUS_PRICES('""" + cmd.upper() + """','""" + st + """','""" + ed + """',0,'""" + adjust_method + """', 'CLOSE') ORDER BY TRADE_DATE
            """
        data_df = self._read_data_DB(query)
        data_df = data_df[data_df.CLOSE_PX > 0.0]

        data_df['CUM_RETURN'] = (data_df.PRICE_RETURN + 1).cumprod()
        data_df['TRADE_DATE'] = pd.to_datetime(data_df.TRADE_DATE)
        data_df.set_index('TRADE_DATE', inplace=True)

        return data_df

    def _get_symbols_prices(self, symbols, st='2018-01-01', ed='2030-01-01', adjust_method='VOLUME'):

        equity_map = {'IF': '000300', 'IH': '000016', 'IC': '000905'}
        for symbol in symbols:
            if symbol in equity_map.keys():
                symbol = equity_map[symbol]
        query = ""
        query = """
            SELECT price.[INSTRUMENT_ID]
                  ,price.[TRADE_DATE]
                  ,price.[OPEN_PX]
                  ,price.[HIGH_PX]
                  ,price.[LOW_PX]
                  ,price.[CLOSE_PX]
                  ,price.[SETTLE_PX]
                  ,price.[VOLUME]
                  ,price.[OPEN_INTEREST]
                  ,price.[TURNOVER]
                  ,price.[UPDATE_TIME]
                  ,instrument.ticker
              FROM [dbo].[DAILY_PRICES] as price
              left join [dbo].[INSTRUMENTS] as instrument 
              on price.[INSTRUMENT_ID] = instrument.[ID]
              where instrument.ticker in (%s) 
              order by price.[TRADE_DATE]
            """ % (','.join(["'" + x + "'" for x in symbols]))

        data_df = self._read_data_DB(query)
        if data_df is not None:
            data_df = data_df[data_df.CLOSE_PX > 0.0]

            # data_df['CUM_RETURN'] = (data_df.PRICE_RETURN + 1).cumprod()
            data_df['TRADE_DATE'] = pd.to_datetime(data_df.TRADE_DATE)
            data_df.set_index('TRADE_DATE', inplace=True)

        return data_df

    def load_market_data(self, symbols, start_date, end_date, **kwargs):
        super().load_market_data(symbols=symbols,start_date=start_date,end_date=end_date,kwargs=kwargs)

        data_df = self._get_symbols_prices(symbols=[symbol.upper() for symbol in symbols],
                                          st=start_date,
                                          ed=end_date)
        if data_df is not None:
            for symbol,data_subdf in data_df.groupby('ticker'):
                _logger.info('Loading daily data for {}'.format(symbol))
                if symbol not in self._data:
                    self._data[symbol]={}
                self._update_datadf(symbol=symbol,freq=self._freq,datadf=data_subdf)
        self._symbols = symbols
        self._get_fields()
        return self

if __name__ == '__main__':
    fc = FutureDailyData()
    retdf = fc._get_symbols_prices(symbols=['rb1910','rb1905'])
    print(retdf)
