#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/8 16:34
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import pandas
from data_engine.data_factory import DataFactory
from common.mongo_object import mongo_object

class Instrument(mongo_object):
    def __init__(self,symbol,asset_type,by_ctp_instrument=False,info=None):
        mongo_object.__init__(self)
        self._symbol = symbol
        self._by_ctp_instrument = by_ctp_instrument
        self._asset_type = asset_type
        if info is not None:
            info['jz_symbol'] = symbol
        self._info = info
        self._data_df = None
        self._data_df_daily = None

        if self._info is None:
            self._load_info()

    def __repr__(self):
        return self.ctp_symbol

    @property
    def data_df_daily(self):
        return self._data_df_daily

    @data_df_daily.setter
    def data_df_daily(self,value):
        self._data_df_daily = value

    @property
    def data_df(self):
        return self._data_df

    @data_df.setter
    def data_df(self,value):
        self._data_df = value

    def _load_info(self):
        if self._symbol is None:
            return self
        mongo_client = self.mongo_client
        db = mongo_client.get_database('MARKET')
        cli = db.get_collection('instruments')
        self._info = cli.find_one({'ASSET_TYPE': self._asset_type,'symbol':self._symbol},{'_id':0})
        return self
        #
        # self._info = DataFactory()._get_instruments()
        # self._info = self._info .xs(self._asset_type,level='ASSET_TYPE')
        # if self._symbol is not None:
        #     self._info = self._info.loc[self._symbol]

    @property
    def jz_symbol(self):
        if self._info is None:
            return None
        if 'jz_symbol' in self._info:
            return self._info['jz_symbol']
        if isinstance(self._info,pandas.Series):
            return str(self._info.name)
        return None

    def is_trading(self,date):
        date = pandas.to_datetime(date).date()
        if self.create_date is None or self.expire_date is None:
            return None
        if date >= pandas.to_datetime(self.create_date).date() and date <= pandas.to_datetime(self.expire_date).date():
            return True
        return False

    @property
    def create_date(self):
        if self._info is None:
            return None
        if isinstance(self._info['CreateDate'],str) and self._info['CreateDate'] == 'None':
            return None
        return self._info['CreateDate']

    @property
    def expire_date(self):
        if self._info is None:
            return None
        if isinstance(self._info['ExpireDate'],str) and self._info['ExpireDate'] == 'None':
            return None
        return self._info['ExpireDate']

    @property
    def ctp_symbol(self):
        if self._info is None or 'InstrumentID0' not in self._info:
            return None
        return str(self._info['InstrumentID0'])

    @property
    def market(self):
        if self._info is None or 'ExchangeID' not in self._info:
            return None
        return str(self._info['ExchangeID'])

    @property
    def tick_size(self):
        if self._info is None:
            return None
        return float(self._info['PriceTick'])

    @property
    def product_id(self):
        if self._info is None:
            return None
        return str(self._info['ProductID'])

    @property
    def contract_size(self):
        if self._info is None:
            return None
        return float(self._info['VolumeMultiple'])


    def calc_feature(self):
        self._feature_name = []
        return self._feature_name