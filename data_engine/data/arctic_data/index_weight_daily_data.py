# -*- coding: utf-8 -*-

import logging
import pandas as pd

import data_engine.setting as Setting
from arctic import Arctic,exceptions
from arctic.date import string_to_daterange
from arctic.exceptions import NoDataFoundException

from data_engine.data.stock_data import StockData
from data_engine.setting import FREQ_1D,INDEX_WEIGHT_LIB
from data_engine.setting import PRICE_TYPE_POST,PRICE_TYPE_PRE,PRICE_TYPE_UN

_logger = logging.getLogger(__name__)

class IndexWeightDailyData(StockData):
    '''
        日线数据接口，从Artic加载
    '''
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        StockData.__init__(self,freq=FREQ_1D,price_type=price_type,**kwargs)

    def _get_arclib_str(self):
        return INDEX_WEIGHT_LIB

    @staticmethod
    def _format_data(df):
        if df is not None:
            df.index = pd.to_datetime(df.index)  # .tz_convert('UTC')
            df.sort_index(inplace=True)
        return df


    def _get_data(self, symbol, start_date, end_date, **kwargs):
        if start_date is None and end_date is None:
            date_range = None
        else:
            date_range = string_to_daterange('_se_'.join([start_date,end_date]),delimiter='_se_')
        if isinstance(symbol,str):
            try:
                df_out = self.arc_lib.read(symbol=symbol, date_range=date_range, columns=None)
                df_out = self._format_data(df_out)
                return df_out
            except NoDataFoundException:
                print(('NoDataFoundException', symbol, '_'.join([start_date, end_date])))
                pass
        elif isinstance(symbol,list):
            df_dict = {}
            for symbol_tmp in symbol:
                try:
                    df_out = self.arc_lib.read(symbol=symbol_tmp, date_range=date_range, columns=None)
                    df_out = self._format_data(df_out)
                    df_dict[symbol_tmp] = df_out
                except NoDataFoundException:
                    print(('NoDataFoundException', symbol, '_'.join([start_date, end_date])))
                    pass
            return df_dict
        return None
