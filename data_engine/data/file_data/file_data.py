# -*- coding: utf-8 -*-

import os
import logging
import pyodbc

import pandas
import pickle
import numpy as np
import pandas as pd

from data_engine.data.market_data import MarketData

import data_engine.setting  as Setting

_logger = logging.getLogger(__name__)

class FileData(MarketData):
    '''
        分钟数据接口，从本地文件加载
    '''
    def __init__(self,freq,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        MarketData.__init__(self,freq=freq,asset_type=None,price_type=price_type,init_arc=False,**kwargs)
        self._folder = None

    @staticmethod
    def _format_data(datadf):
        if 'settle' not in datadf.columns:
            datadf['settle'] = datadf['close']
        for nextcol, col in {'next_close':'close',
                             'next_open': 'open',
                             'next_high': 'high',
                             'next_low': 'low',
                             'next_settle': 'settle',
                             }.items():
            if nextcol not in datadf.columns:
                datadf.loc[:,nextcol] = datadf[col].shift(-1)
        for nextcol, col in {'last_close':'close',
                             'last_open': 'open',
                             'last_high': 'high',
                             'last_low': 'low',
                             'last_settle': 'settle',
                             }.items():
            if nextcol not in datadf.columns:
                datadf.loc[:,nextcol] = datadf[col].shift(1)
        return datadf

    def _get_data(self, symbol, start_date=None, end_date=None, **kwargs):
        '''
        从本地文件加载
        :return:
        '''
        price_type = self._price_type
        if 'price_type' in kwargs:
            price_type = kwargs['price_type']
        freq = self._freq
        folder = self._folder
        if folder is None:
            return None

        if start_date is not None:
            start_date = MarketData.to_timestamp(start_date,tz=Setting.DEFAULT_TIMEZONE)
        if end_date is not None:
            end_date = MarketData.to_timestamp(end_date,tz=Setting.DEFAULT_TIMEZONE)
        if isinstance(symbol,str):
            file = os.path.join(folder, '_'.join([symbol, price_type]) + '.csv')
            data_df = None
            if os.path.exists(file.replace('.csv','.pkl')):
                with open(file.replace('.csv','.pkl'),'rb') as handle:
                    data_df = pickle.load(handle)
            elif os.path.exists(file):
                data_df = pandas.DataFrame.from_csv(file)
            if data_df is not None and not data_df.empty:
                # if symbol not in self._data:
                #     self._data[symbol] = {}
                if start_date is not None:
                    data_df = data_df[ data_df.index >= start_date]
                if end_date is not None:
                    data_df = data_df[ data_df.index <= end_date]
                return self._format_data(data_df)
        elif isinstance(symbol,list):
            df_dict = {}
            count = 0
            for symbol_tmp in symbol:
                file = os.path.join(folder, '_'.join([symbol_tmp, price_type]) + '.csv')
                data_df = None
                if os.path.exists(file.replace('.csv','.pkl')):
                    count += 1
                    # print('get_data',symbol_tmp,'%s / %s' %(count,len(symbol)))
                    with open(file.replace('.csv','.pkl'),'rb') as handle:
                        data_df = pickle.load(handle)
                elif os.path.exists(file):
                    count += 1
                    # print('get_data',symbol_tmp,'%s / %s' %(count,len(symbol)))
                    data_df = pandas.DataFrame.from_csv(file)
                if data_df is not None and not data_df.empty:
                    # if symbol_tmp not in self._data:
                    #     self._data[symbol_tmp] = {}
                    if start_date is not None:
                        data_df = data_df[ data_df.index >= start_date]
                    if end_date is not None:
                        data_df = data_df[ data_df.index <= end_date]
                    df_dict[symbol_tmp] = data_df
            df_dict = { x: self._format_data(y) for x,y in df_dict.items()}
            return df_dict
        return None

