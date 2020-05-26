# -*- coding: utf-8 -*-

import logging
import pandas
import datetime

from arctic import Arctic,exceptions
from arctic.date import string_to_daterange
from arctic.chunkstore import chunkstore

import data_engine.setting as Setting
from data_engine.data.stock_data import StockData
from data_engine.setting import FREQ_1D,STOCK_DAILY_LIB,STOCK_DAILY_POST_LIB,STOCK_DAILY_PRE_LIB,STOCK_DAILY_JQ_LIB,STOCK_DAILY_POST_JQ_LIB,STOCK_DAILY_PRE_JQ_LIB
from data_engine.setting import PRICE_TYPE_POST,PRICE_TYPE_PRE
from data_engine.setting import DEFAULT_TIMEZONE

_logger = logging.getLogger(__name__)

class StockFeatureDailyData(StockData):
    '''
        日线数据接口，从Artic加载
    '''
    def __init__(self,**kwargs):
        StockData.__init__(self,freq=FREQ_1D,**kwargs)

    def _get_arctic_symbols(self,symbols):
        return symbols

    def _get_arclib_str(self):
        return Setting.FEATURE_DAILY_LIB

    def _get_data(self, symbol, start_date, end_date, **kwargs):
        if start_date is None and end_date is None:
            chunk_range = None
        else:
            chunk_range = string_to_daterange('_se_'.join([start_date,end_date]),delimiter='_se_')
        try:

            if isinstance(symbol,str):
                ret_symbols = self.arc_lib.list_symbols(partial_match=symbol)
                series_dict = self.arc_lib.read(ret_symbols,chunk_range)
                return {symbol,pandas.concat(series_dict.values(),axis=1)}
            elif isinstance(symbol,list):
                print('get_date..','_'.join(symbol))
                t1 = datetime.datetime.now()
                df_dict = {}
                for each in symbol:
                    ret_symbols = self.arc_lib.list_symbols(partial_match=each)
                    series_dict = self.arc_lib.read(ret_symbols,chunk_range)
                    df = pandas.concat(series_dict.values(), axis=1)
                    df_dict[each] = df
                print('get_date',datetime.datetime.now()-t1)
                return df_dict
        except exceptions.NoDataFoundException:
            pass
        return None
