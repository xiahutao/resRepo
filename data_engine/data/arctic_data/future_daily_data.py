# -*- coding: utf-8 -*-

import datetime
import pandas
from arctic import Arctic

from data_engine.data.future_data import FutureData
from data_engine.setting import FREQ_1D,FUTURE_DAILY_LIB,DEFAULT_TIMEZONE
import data_engine.setting as Setting
from arctic.date import DateRange

class FutureDailyData(FutureData):
    '''
        日线数据接口，从Arctic加载
    '''
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        FutureData.__init__(self, freq=FREQ_1D,price_type=price_type,**kwargs)

    def _get_arclib_str(self):
        return FUTURE_DAILY_LIB

    @staticmethod
    def _format_data(df):
        if df is not None and not df.empty:
            df['trade_date'] = pandas.DatetimeIndex(df.index).date
            df.index = (pandas.to_datetime(pandas.DatetimeIndex(df.index).date) + datetime.timedelta(hours=15)).tz_localize(DEFAULT_TIMEZONE)
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated()]
            if 'last_settle' not in df.columns:
                df['last_settle'] = df['settle'].shift(1)
        return df
    def _get_data(self, symbol, start_date, end_date, **kwargs):
        df = None
        if start_date is None:
            start_date = '1990-01-01'
        if end_date is None:
            end_date = '2199-01-01'
        start_date = pandas.to_datetime(start_date)
        start_date_1 = pandas.to_datetime(start_date) + datetime.timedelta(days=-0)
        end_date = pandas.to_datetime(end_date)

        chunk_range = DateRange(start_date_1.replace(hour=0,minute=0,second=0,microsecond=0,tzinfo=None), end_date.replace(hour=23,minute=59,second=59,microsecond=0,tzinfo=None))

        if isinstance(symbol, str):
            df = self.arc_lib.read(symbol, chunk_range)
            if df is not None:
                return FutureDailyData._format_data(df)
        elif isinstance(symbol, list) and len(symbol) == 1:
            df = self.arc_lib.read(symbol[0], chunk_range)
            if df is not None:
                return FutureDailyData._format_data(df)
        elif isinstance(symbol,list):
            df_dict = self.arc_lib.read(symbol,chunk_range)
            if df_dict is None:
                return None
            elif isinstance(df_dict, dict):
                df_dict = {x: FutureDailyData._format_data(y) for x,y in df_dict.items() if not y.empty}
                return df_dict
            elif isinstance(df_dict, pandas.DataFrame):
                return FutureDailyData._format_data(df_dict)
        return None
