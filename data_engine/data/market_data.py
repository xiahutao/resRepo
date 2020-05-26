# -*- coding: utf-8 -*-
import os
import pandas
from abc import abstractmethod
import logging
import pytz
import pymongo
import datetime
import pickle
from arctic import Arctic,exceptions
from arctic.date import string_to_daterange
from common.os_func import check_fold

import data_engine.setting as Setting

from multiprocessing import Queue,Process,Pool,cpu_count

class MarketData(object):
    '''
        数据接口的基类
        _data: 数据字典对象， symbol_freq_datadf  市场量价数据
        _feature_data 数据字典对象， symbol_freq_datadf  特征/指标数据
        _metadata: 元数据字典对象， symbol_freq_dict
        _freq: 1d,1m
        _asset_type: future
        _symbols: _data.keys()
        _fields: _data中datadf的列头
    '''
    _logger = logging.getLogger(__name__)

    def __init__(self,freq,asset_type,cpu=1,price_type=Setting.PRICE_TYPE_UN,init_arc = True,**kwargs):
        self._price_type = price_type
        self._symbols = []
        self._fields = []
        self._data = {}
        self._feature_data = {}
        self._metadata = {}
        self._freq = freq
        self._asset_type = asset_type

        self._process_queue = Queue()
        # self._pool = Pool(cpu)
        self.arc = None
        self.arc_lib=None
        self._lib_str = self._get_arclib_str()
        if init_arc:
            self.arc = Arctic(self.get_mongo_client())
            if self._lib_str is not None:
                self.arc_lib = self.arc.get_library(self._lib_str)

    def class_name(self):
        return self.__class__.__name__

    def has_data(self,key):
        if isinstance(key,str):
            key_tmp = (key,self._freq,Setting.PRICE_TYPE_UN)
        elif len(key) > 2:
            key_tmp = (key[0], key[1] ,key[2])
        elif len(key)>1:
            key_tmp = (key[0],self._freq,key[1])
        else:
            print(key)
        if key_tmp not in self._data:
            return False
        return True

    def pop(self,key):
        if isinstance(key,str):
            key_tmp = (key,self._freq,Setting.PRICE_TYPE_UN)
        elif len(key) > 2:
            key_tmp = (key[0], key[1] ,key[2])
        elif len(key)>1:
            key_tmp = (key[0],self._freq,key[1])
        else:
            print(key)
        if key_tmp not in self._data:
            return None
        else:
            self._data.pop(key_tmp)
        return None

    def __getitem__(self, key):
        if isinstance(key,str):
            key_tmp = (key,self._freq,Setting.PRICE_TYPE_UN)
        elif len(key) > 2:
            key_tmp = (key[0], key[1] ,key[2])
        elif len(key)>1:
            key_tmp = (key[0],self._freq,key[1])
        else:
            print(key)
        if key_tmp not in self._data:
            print(('no market data',key_tmp))
            return None
        return self._data[key_tmp]

    def get_data(self,symbol,price_type=Setting.PRICE_TYPE_UN,use_start_time_as_index=False):
        key_tmp = (symbol, self._freq, price_type)
        if key_tmp not in self._data:
            print(('no market data',key_tmp))
            return None
        df = self._data[key_tmp]
        if use_start_time_as_index and 'start_time' in df.columns:
            df.index = pandas.DatetimeIndex(df['start_time'], tz=pytz.timezone(Setting.DEFAULT_TIMEZONE))
            df.index.name = 'index'
        return df

    def _get_arclib_str(self):
        return None

    def get_panel(self,value_column,freq):
        if isinstance(value_column,str):
            return self._get_panel_by_col(value_column=value_column,freq=freq)
        if isinstance(value_column,list):
            panel_dict = {}
            for col in value_column:
                panel = self._get_panel_by_col(value_column=col,freq=freq)
                panel_dict[col] = panel
            return panel_dict
        return None

    def _get_panel_by_col(self,value_column,freq):
        return_series_list = []
        for (symbol,freq_tmp,price_type) in self._data.keys():
            if freq_tmp != freq:
                continue
            if self._data[(symbol,freq_tmp,price_type)] is not None:
                if value_column not in self._data[(symbol,freq_tmp,price_type)].columns:
                    continue
                value_series = self._data[(symbol, freq_tmp, price_type)][value_column]
                value_series.name = symbol
                return_series_list.append(value_series)
        return_dataframe = pandas.concat(return_series_list,axis=1)
        return return_dataframe

    def _get_fields(self):
        pass
        # for (symbol,freq,price_type) in self._data.keys():
        #     if self._data[(symbol,freq,price_type)] is not None:
        #         self._fields = list(self._data[(symbol,freq,price_type)].columns)
        #         return

    def _checkmetadata(self,symbol,freq, start_date, end_date,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        if (symbol,freq,price_type) not in self._metadata:
            return False
        if 's' not in self._metadata[(symbol,freq,price_type)] or 'e' not in self._metadata[(symbol,freq,price_type)]:
            return False

        start_date = MarketData.to_timestamp(start_date,tz=Setting.DEFAULT_TIMEZONE)
        end_date = MarketData.to_timestamp(end_date,tz=Setting.DEFAULT_TIMEZONE)
        if start_date < pandas.to_datetime(self._metadata[(symbol,freq,price_type)]['s']):
            return False
        if end_date > pandas.to_datetime(self._metadata[(symbol,freq,price_type)]['e']):
            return False

        return True

    def _update_datadf(self,symbol,freq,datadf,price_type=Setting.PRICE_TYPE_UN):
        if datadf is None or datadf.empty:
            print('datadf is empty',symbol,'_update_datadf')
            return None
        self._data[(symbol,freq,price_type)] = datadf
        metadata = {'len':len(datadf),'s':datadf.iloc[0].name,'e':datadf.iloc[-1].name,'symbol':symbol,'freq':freq}
        self._metadata[(symbol,freq,price_type)] = metadata

    @staticmethod
    def to_timestamp(date,tz=Setting.DEFAULT_TIMEZONE):
        if isinstance(date,str):
            if tz is not None:
                date = pandas.Timestamp(date,tz=tz)
            else:
                date = pandas.Timestamp(date)
        elif isinstance(date,pandas.Timestamp) \
                and date.tzinfo is None and tz is not None:
            date = date.tz_localize(tz)
        return date

    @abstractmethod
    def _get_data(self, symbol, start_date, end_date,price_type=Setting.PRICE_TYPE_UN, **kwargs):
        return None

    @abstractmethod
    def _get_feature_data(self, symbol, start_date, end_date, **kwargs):
        #todo, 加载特征/指标数据
        return None

    @staticmethod
    def get_mongo_client_bak():
        """
        获取Mongo库客户端对象
        :return:
        """
        if Setting.MONGDB_USER is not None and Setting.MONGDB_PW is not None:
            # mongoclient = pymongo.MongoClient(
            #     'mongodb://%s:%s@%s:27017/' % (Setting.MONGDB_USER, Setting.MONGDB_PW, Setting.MONGDB_IP))
            mongoclient = pymongo.MongoClient(Setting.MONGDB_IP, username=Setting.MONGDB_USER,
                                              password=Setting.MONGDB_PW)

            #如果这句代码报错，通常意味着数据库的用户名密码错误
            # mongoclient.admin.command('ismaster')
        else:
            mongoclient = pymongo.MongoClient(Setting.MONGDB_IP)
        return mongoclient

    @staticmethod
    def __get_mongo_conn_url_replicaset(ip_port_list, user=None, pwd=None, set_name=None):
        url = 'mongodb://'
        if user is not None:
            if pwd is None:
                pwd = user
            url += '%s:%s@' % (user, pwd)
        url += ','.join(ip_port_list)
        if set_name is not None:
            url += '/?replicaSet=%s&readPreference=secondaryPreferred' % set_name
        return url

    @staticmethod
    def get_mongo_client():
        """
        获取Mongo库客户端对象
        :return:
        """
        return MarketData.get_mongo_client_bak()
        if Setting.MONGDB_USER is not None and Setting.MONGDB_PW is not None:
            if Setting.MONGDB_IP_list is not None and isinstance(Setting.MONGDB_IP_list,list):
                conn_url = MarketData.__get_mongo_conn_url_replicaset(Setting.MONGDB_IP_list,user=Setting.MONGDB_USER,pwd=Setting.MONGDB_PW,set_name=Setting.MONGDB_SET_NAME)
                mongoclient = pymongo.MongoClient(conn_url)
            else:
                # mongoclient = pymongo.MongoClient(
                #     'mongodb://%s:%s@%s:27017/' % (Setting.MONGDB_USER, Setting.MONGDB_PW, Setting.MONGDB_IP))
                mongoclient = pymongo.MongoClient(Setting.MONGDB_IP, username=Setting.MONGDB_USER,
                                                  password=Setting.MONGDB_PW)

            #如果这句代码报错，通常意味着数据库的用户名密码错误
            # mongoclient.admin.command('ismaster')
        else:
            mongoclient = pymongo.MongoClient(Setting.MONGDB_IP)
        return mongoclient
    def _get_arctic_symbols(self,symbols):
        ret_symbols = []
        try:
            for each in symbols:
                if self.arc_lib.has_symbol(each.upper()):
                    ret_symbols.append(each.upper())
                elif self.arc_lib.has_symbol(each):
                    ret_symbols.append(each)
                elif self.arc_lib.has_symbol(each.lower()):
                    ret_symbols.append(each)
        except:
            return symbols
        return ret_symbols

    def load_market_data(self, symbols, start_date, end_date, **kwargs):
        '''
        加载数据
        :param symbols:
        :param start_date:
        :param end_date:
        :param kwargs:
        :return:
        '''
        self._logger.debug(msg=self.__class__.__module__)
        self._symbols = self._get_arctic_symbols(symbols=symbols)
        if start_date is None:
            start_date = datetime.date(1990,1,1) # '1990-01-01'
        if end_date is None:
            end_date = datetime.date(2199,1,1) # '2199-01-01'
        try:
            if isinstance(end_date,datetime.date) and not isinstance(end_date,pandas.Timestamp) :
                end_date = (pandas.to_datetime(end_date) + datetime.timedelta(hours=15)).tz_localize(Setting.DEFAULT_TIMEZONE)
        except:
            pass
        try:
            if isinstance(start_date,datetime.date) and not isinstance(start_date,pandas.Timestamp):
                start_date = (pandas.to_datetime(start_date) + datetime.timedelta(hours=15)).tz_localize(Setting.DEFAULT_TIMEZONE)
        except:
            pass
        symbols_toload = []
        for symbol in self._symbols:
            if self._checkmetadata(symbol=symbol,freq=self._freq,price_type=self._price_type,start_date=start_date,end_date=end_date):
                continue
            symbols_toload.append(symbol)
        if len(symbols_toload)>0:
            t1 = datetime.datetime.now()
            datadf = self._get_data(symbols_toload, start_date, end_date, **kwargs)
            # print(self.class_name(), 'Loading %s data '% self._freq,datetime.datetime.now() - t1)
            if datadf is None:
                pass
            elif isinstance(datadf,dict):
                for symbol_tmp, datadf_tmp in datadf.items():
                    self._update_datadf(symbol=symbol_tmp,freq=self._freq,datadf=datadf_tmp,price_type=self._price_type)
            elif isinstance(datadf,pandas.DataFrame):
                if isinstance(symbols_toload,str):
                    self._update_datadf(symbol=symbols,freq=self._freq,datadf=datadf,price_type=self._price_type)
                elif isinstance(symbols_toload,list):
                    self._update_datadf(symbol=symbols_toload[0],freq=self._freq,datadf=datadf,price_type=self._price_type)
        self._symbols = symbols
        self._get_fields()
        return self

    def load_feature_data(self, symbols, start_date, end_date, **kwargs):
        '''
        todo, 加载特征/指标数据
        :param symbols:
        :param start_date:
        :param end_date:
        :param kwargs:
        :return:
        '''
        return self


    def SaveToLocal_File(self,symbol=None,price_type=Setting.PRICE_TYPE_UN):
        '''
        保存数据到本地文件
        :return:
        '''
        folder = None
        for (symbol_key,freq,price_type_tmp),data_df in self._data.items():
            if price_type_tmp != price_type:
                continue
            if data_df is None or data_df.empty:
                continue
            if freq == '1d':
                folder = Setting.DAILY_FILE_FOLDER
            elif freq == '5m':
                folder = Setting.MINUTE_FILE_SOURCE_5M
            elif freq == '1m':
                folder = Setting.MINUTE_FILE_SOURCE
            elif freq == 'tick':
                folder = Setting.TICK_FILE_SOURCE
            else:
                continue
            if isinstance(symbol,str):
                if symbol is not None and symbol != symbol_key:
                    continue
            elif isinstance(symbol,list):
                if symbol_key not in symbol:
                    continue
            check_fold(folder)
            file = os.path.join(folder, '_'.join([symbol_key,price_type_tmp]) + '.csv')
            print(('SaveToLocal_File',file))
            with open(file.replace('.csv','.pkl'),'wb') as handle:
                pickle.dump(data_df,handle)
            # data_df.to_csv(file)
