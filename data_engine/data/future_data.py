# -*- coding: utf-8 -*-

import pyodbc
import pandas as pd

import data_engine.setting as Setting

from data_engine.data.market_data import MarketData
from data_engine.setting import ASSETTYPE_FUTURE

class FutureData(MarketData):
    '''
        用于期货的数据接口基类
    '''
    def __init__(self,freq,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        MarketData.__init__(self,freq=freq,asset_type=ASSETTYPE_FUTURE,price_type=price_type,**kwargs)

    def load_market_info(self):

        query = """
                SELECT RTRIM(I.TICKER) AS TICKER, C.CONTRACT_SIZE, C.TICK_SIZE,C.BROKER_FEE, 
                C.BROKER_FEE_RATE, C.INTRADAY_BROKER_FEE, C.INTRADAY_BROKER_FEE_RATE, C.MARGIN_RATE, M.* 
                
                FROM [MARKET].[dbo].[FUTURES_CONTRACTS_CITIC] C,[MARKET].[dbo].INSTRUMENTS I,[MARKET].[dbo].MARKETS M 
                
                WHERE C.UNDERLYING_ID=I.ID AND I.MARKET_ID=M.ID 
                
                ORDER BY M.CODE,TICKER
                """
        try:
            conn = pyodbc.connect(Setting.DB_CONFIG_DAILY)
        except:
            self._logger.error("DB connection error: read, " + Setting.DB_CONFIG_DAILY)

        try:
            mkt_info = pd.read_sql(query, conn)
            mkt_info['TICKER'] = mkt_info.TICKER.replace('000300', 'IF')
            mkt_info['TICKER'] = mkt_info.TICKER.replace('000016', 'IH')
            mkt_info['TICKER'] = mkt_info.TICKER.replace('000905', 'IC')
            mkt_info.set_index('TICKER', inplace=True)
        except:
            self._logger.error("read failed:", query)

        return mkt_info

    # @staticmethod
    # def get_trading_session():
    #     trd_ss = pd.read_csv(setting.FILE_TRADING_SESSION,
    #                          dtype={'Session1_Start': str, 'Session1_End': str, 'Session2_Start': str,
    #                                 'Session2_End': str, 'Session3_Start': str, 'Session3_End': str,
    #                                 'Session4_Start': str, 'Session4_End': str})
    #     return trd_ss.set_index('Market').fillna('')
