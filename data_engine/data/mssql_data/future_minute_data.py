# -*- coding: utf-8 -*-

import os
import logging
import pyodbc
import numpy as np
import pandas as pd

from data_engine.data.future_data import FutureData
from data_engine.setting import FREQ_1M

import data_engine.setting as Setting

_logger = logging.getLogger(__name__)

class FutureMinuteData(FutureData):
    '''
        分钟数据接口，从MSSQL加载
    '''
    def __init__(self,**kwargs):
        FutureData.__init__(self,freq=FREQ_1M,**kwargs)

    def _convert_from_minute_data(self, data, period = 5, open_adj_ratio = 0.2, base=0):
        if data.shape[0] == 0:
            return data.copy()

        data_copy = data.copy()
        data_copy.set_index('start_time', inplace=True)
        data_copy.loc[:, 'vol2']  = data_copy.volumn + 1e-10
        data_copy.loc[:, 'px_vol'] = data_copy.vol2 * data_copy.vwap
        data_copy.loc[:, 'norm']   = data_copy['return'] + 1
        data_convert = data_copy.resample(str(period) + 'min', base=base).agg \
            ({'exchange_code' :'first', 'contract_id' :'first' ,'end_time' :'last', 'open' :'first' , 'high' :'max', 'low' :'min', 'close' :'last', 'twap' :'mean', 'px_vol' :'sum', 'vol2' :'sum', 'cnt_tick' :'sum', 'up_tick' :'sum',  'down_tick' :'sum', 'volumn' :'sum', 'up_volumn' :'sum', 'down_volumn' :'sum', 'open_int' :'last', 'norm' :'prod', 'px_delta' :'sum'})
        data_convert.loc[:, 'vwap'] = data_convert.px_vol / data_convert.vol2
        data_convert.loc[:, 'return'] = data_convert.norm - 1

        timedelta = -np.timedelta64( int(period *open_adj_ratio), 'm')

        data_copy.loc[:, 'open_adj_time'] = data_copy.index
        data_adj = data_copy[['open', 'open_adj_time']].shift(freq=timedelta).resample(str(period) + 'min').first()
        data_adj.columns = ['open_adj', 'open_adj_time']

        data_convert = data_convert.join(data_adj, how='left' )
        data_convert = data_convert[~data_convert.close.isnull()]
        data_convert.reset_index(inplace=True)
        data_convert = data_convert[['exchange_code', 'contract_id', 'start_time', 'end_time', 'open', 'high', 'low', 'close', 'open_adj', 'open_adj_time', 'vwap', 'twap', 'cnt_tick', 'up_tick', 'down_tick', 'volumn', 'up_volumn', 'down_volumn', 'open_int', 'return', 'px_delta']].dropna(thresh=5)
        return data_convert

    def _load_intraday_min_data(self, mkt, st, ed, tableName):

        conn = pyodbc.connect(Setting.DB_CONFIG_MINUTE, autoCommit = True)
        sql = "select [end_time], [return], [close], [market] from MARKET.dbo." + tableName + " where is_active = 1 and market = '" + mkt + "' and start_time > ' "+ st + "' and start_time < ' "+ ed + "'"
        data_df = pd.read_sql(sql, conn).sort_values(['end_time'])
        data_df.rename \
            (columns={'end_time' :'DATE_TIME', 'return' :'PRICE_RETURN', 'close' :'CLOSE_PX', 'market' :'MARKET'}, inplace=True)
        data_df['DATE_TIME'] = pd.to_datetime( data_df.DATE_TIME )

        data_df.set_index('DATE_TIME', inplace=True)

        return data_df

    def _load_intraday_data_converted(self, mkt, st, ed, period=1, tableName='FUTURE_MINUTES'):

        conn = pyodbc.connect(Setting.DB_CONFIG_MINUTE, autoCommit=True)
        sql = "select * from MARKET.dbo." + tableName + " where is_active = 1 and market = '" + mkt + "' and start_time > '" + st + "' and start_time < '" + ed + "'"
        data_df = pd.read_sql(sql, conn).sort_values(['market', 'start_time']).drop(
                ['market', 'is_active', 'update_time'], axis=1)

        if period > 1:
            data_df = self._convert_from_minute_data(data_df, period=period, open_adj_ratio=0.2)

        data_df.rename(
            columns={'end_time': 'DATE_TIME', 'open': 'OPEN', 'high': 'HIGH', 'low': 'LOW', 'close': 'CLOSE_PX',
                     'return': 'PRICE_RETURN', 'volumn': 'VOLUME', 'px_delta': 'PRICE_DELTA'}, inplace=True)
        data_df['DATE_TIME'] = pd.to_datetime(data_df.DATE_TIME)
        data_df['TIME'] = data_df['DATE_TIME'].apply(lambda x: x.time())
        data_df['DATE'] = data_df['DATE_TIME'].apply(lambda x: x.date())
        data_df['CUM_RETURN'] = (data_df.PRICE_RETURN + 1).cumprod()
        data_df.set_index('DATE_TIME', inplace=True)

        return data_df

    def load_market_data(self, symbols, start_date, end_date, **kwargs):
        super().load_market_data(symbols=symbols,start_date=start_date,end_date=end_date,kwargs=kwargs)
        for symbol in symbols:
            _logger.info('Loading minute data for {}'.format(symbol))
            self._data[symbol][self._freq] = self._load_intraday_data_converted(symbol, start_date, end_date, **kwargs)

        self._symbols = symbols
        self._get_fields()
        return self
