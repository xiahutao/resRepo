# -*- coding: utf-8 -*-

import logging
import pandas
import datetime

from arctic import Arctic,exceptions
from arctic.date import string_to_daterange

import data_engine.setting as Setting
from data_engine.data.stock_data import StockData
from data_engine.setting import FREQ_1D,STOCK_DAILY_LIB,STOCK_DAILY_POST_LIB,STOCK_DAILY_PRE_LIB,STOCK_DAILY_JQ_LIB,STOCK_DAILY_POST_JQ_LIB,STOCK_DAILY_PRE_JQ_LIB
from data_engine.setting import PRICE_TYPE_POST,PRICE_TYPE_PRE
from data_engine.setting import DEFAULT_TIMEZONE

_logger = logging.getLogger(__name__)

class StockDailyData(StockData):
    '''
        日线数据接口，从Artic加载
    '''
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        StockData.__init__(self,freq=FREQ_1D,price_type=price_type,**kwargs)

    def _get_arclib_str(self):
        if self._price_type == PRICE_TYPE_PRE:
            return STOCK_DAILY_PRE_JQ_LIB
        elif self._price_type == PRICE_TYPE_POST:
            return STOCK_DAILY_POST_JQ_LIB
        else:
            return STOCK_DAILY_JQ_LIB

    @staticmethod
    def _format_data(df):
        if df is not None and not df.empty:
            df = StockData._format_data(df)
            df['trade_date'] = pandas.DatetimeIndex(df.index).date
            df.index = (pandas.to_datetime(pandas.DatetimeIndex(df.index).date) + datetime.timedelta(hours=15)).tz_localize(DEFAULT_TIMEZONE)
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated()]
        return df

    def _get_data(self, symbol, start_date, end_date, **kwargs):
        if start_date is None and end_date is None:
            chunk_range = None
        else:
            chunk_range = string_to_daterange('_se_'.join([ pandas.to_datetime(start_date).strftime('%Y-%m-%d'),pandas.to_datetime(end_date).strftime('%Y-%m-%d')]),delimiter='_se_')
        try:
            if isinstance(symbol,str):
                df = None
                if self.arc_lib.has_symbol(symbol):
                    df = self.arc_lib.read(symbol,chunk_range)
                elif self.arc_lib.has_symbol(symbol.lower()):
                    df = self.arc_lib.read(symbol.lower(),chunk_range)

                if df is not None:
                    df = self._format_data(df)
                    return df
            elif isinstance(symbol,list):
                print('get_date..','_'.join(symbol))
                t1 = datetime.datetime.now()
                df_dict = self.arc_lib.read(symbol,chunk_range)
                print('get_date',datetime.datetime.now()-t1)
                if df_dict is None:
                    return None
                elif isinstance(df_dict,dict):
                    df_dict = {x: StockDailyData._format_data(y) for x, y in df_dict.items() if not y.empty}
                    return df_dict
                elif isinstance(df_dict,pandas.DataFrame):
                    df_dict = self._format_data(df_dict)
                    return df_dict
        except exceptions.NoDataFoundException:
            return None
