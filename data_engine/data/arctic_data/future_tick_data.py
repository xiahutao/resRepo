# -*- coding: utf-8 -*-

import logging
import pandas as pd
import data_engine.setting as Setting
from arctic import Arctic,exceptions
from arctic.date import string_to_daterange

from data_engine.data.future_data import FutureData
from data_engine.setting import FREQ_TICK,FUTURE_TICK_LIB,DEFAULT_TIMEZONE
from arctic.exceptions import NoDataFoundException

_logger = logging.getLogger(__name__)

class FutureTickData(FutureData):
    '''
        分钟数据接口，从MSSQL加载
    '''
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        FutureData.__init__(self,freq=FREQ_TICK,price_type=price_type,**kwargs)

    def _get_arclib_str(self):
        return FUTURE_TICK_LIB

    @staticmethod
    def _format_data(df):
        if df is not None and not df.empty:
            df.index = pd.to_datetime(df.index).tz_convert(DEFAULT_TIMEZONE)
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated()]
        return df

    def __read_data(self,symbol,date_range,columns):
        try:
            ret = self.arc_lib.read(symbol=symbol, date_range=date_range, columns=columns)
            return ret
        except NoDataFoundException:
            return None

    def _get_data(self, symbol, start_date, end_date, **kwargs):

        if start_date is None:
            start_date = self.arc_lib.min_date(symbol=symbol).strftime('%Y%m%d')
        if end_date is None:
            end_date = self.arc_lib.max_date(symbol=symbol).strftime('%Y%m%d')
        date_range = string_to_daterange('_se_'.join([start_date,end_date]),delimiter='_se_')
        if isinstance(symbol,str):
            try:
                df_out = self.__read_data(symbol=symbol, date_range=date_range, columns=None)
                df_out = self._format_data(df_out)
                return df_out
            except NoDataFoundException:
                print(('NoDataFoundException', symbol, '_'.join([start_date, end_date])))
                pass
        elif isinstance(symbol,list):
            df_dict = {}
            for symbol_tmp in symbol:
                try:
                    df_out = self.__read_data(symbol=symbol_tmp, date_range=date_range, columns=None)
                    df_out = self._format_data(df_out)
                    df_dict[symbol_tmp] = df_out
                except NoDataFoundException:
                    print(('NoDataFoundException', symbol, '_'.join([start_date, end_date])))
                    pass
            return df_dict
        return None

    def get_symbols(self):
        return self.arc_lib.list_symbols()

    def max_date(self,symbol):
        return self.arc_lib.max_date(symbol=symbol)

    def min_date(self,symbol):
        return self.arc_lib.min_date(symbol=symbol)
