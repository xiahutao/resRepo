#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/3/9 16:00
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import datetime
import pandas
import numpy
import data_engine.global_variable as global_variable
from data_engine.instrument.instrument import Instrument
from common.mongo_object import mongo_object
from data_engine.data_factory import DataFactory
from data_engine.instrument.future import Future

class Product(mongo_object):
    def __init__(self,product_id):
        mongo_object.__init__(self)
        self._product_id = product_id.upper().replace('_VOL','').replace('_S','')
        self._info = None
        self._trading_sessions = None

        self._futures={}

        self.__load().load_futures()

    def __repr__(self):
        return self.product_id

    def load_trading_sessions(self):
        self._trading_sessions = DataFactory._get_trading_sessions(product_ids=self.product_id)
        return self

    def get_trading_future(self,date=None):
        if date is None:
            date = global_variable.get_now()
        ret = {}
        for symbol,fut in self._futures.items():
            if isinstance(fut,Future):
                is_trading = fut.is_trading(date=date)
                if is_trading is None or not is_trading:
                    continue
                ret[symbol] = fut
        return ret

    @staticmethod
    def yield_product():
        list_product = Product.list_product()
        for pro in list_product:
            yield Product(pro)

    def get_trading_sessions(self,bydate=None):
        if self._trading_sessions is None:
            self.load_trading_sessions()
        if self._trading_sessions is None:
            return None
        if bydate is None:
            bydate = datetime.datetime.now().today()
        else:
            bydate = pandas.to_datetime(bydate).strftime('%Y%m%d')
        if isinstance(self._trading_sessions, pandas.DataFrame):
            ret = self._trading_sessions[ (self._trading_sessions['DateRange_Start'] <= pandas.to_datetime(bydate)) &  (self._trading_sessions['DateRange_End'] >= pandas.to_datetime(bydate))  ]
            if ret.empty:
                return None
            return ret.iloc[-1]
        elif isinstance(self._trading_sessions,pandas.Series):
            ret = self._trading_sessions
            return ret
        return None

    def __load(self):
        '''
        加载基础信息
        :return:
        '''
        if self._product_id is None:
            return self
        mongo_client = self.mongo_client
        db = mongo_client.get_database('MARKET')
        cli = db.get_collection('product')
        self._info = cli.find_one({'ProductID': self._product_id},{'_id':0})
        return self

    @property
    def tick_size(self):
        if self._info is None :
            return None
        return float(self._info['PriceTick'])

    @property
    def product_id(self):
        return self._product_id

    @property
    def ctp_product_id(self):
        if self._info is None :
            return None
        return self._info['ctp_product_id']

    @property
    def contract_size(self):
        if self._info is None :
            return None
        return float(self._info['VolumeMultiple'])

    @property
    def exchange_id(self):
        if self._info is None:
            return None
        if 'ExchangeID' not in self._info:
            return None
        return self._info['ExchangeID']

    @property
    def sector(self):
        if self._info is None:
            return None
        if 'Sector' not in self._info:
            return None
        return self._info['Sector']

    def load_futures(self):
        if self._info is not None:
            futs = self._info['contract']
            for sym,info in futs.items():
                fut = Future(symbol=sym,info=info)
                self._futures[sym] = fut
        return self

    def list_futures(self):
        return list(self._futures.values())

    def get_future(self,symbol):
        if symbol not in self._futures:
            return None
        return self._futures[symbol]

    def list_symbols(self):
        if self._info is not None:
            futs = self._info['contract']
            return set(futs.keys())
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('MARKET')
        cli = db.get_collection('instruments')
        result = cli.find({'ASSET_TYPE':global_variable.ASSETTYPE_FUTURE,'ProductID':self.ctp_product_id},{'_id':0,'symbol':1})
        result_list = list(result[:])
        if result_list is None:
            return None
        return [x['symbol'] for x in result_list]

    def load_hq(self,date=None,freq=global_variable.FREQ_1D,only_trading=True):
        if date is None:
            date = global_variable.get_now()
        for _,fut in self._futures.items():
            if only_trading and not fut.is_trading(date=date):
                continue
            fut.load_hq(end_date=date,freq=freq)

    def get_hq_panel(self,date = None,only_trading=True):
        series_list = []
        if date is None:
            date = global_variable.get_now()
        for sym,fut in self._futures.items():
            if only_trading and not fut.is_trading(date=date):
                continue
            hq_df = fut._hq_df.loc[:date,:]
            series = hq_df.iloc[-1]
            series['datetime'] = series.name
            series.name = sym
            series_list.append(series)
        df = pandas.concat(series_list,axis=1).T
        if df is not None and not df.empty:
            for col in ['open','high','close','low','settle','volume','open_int']:
                if col in df.columns:
                    df[col] = df[col].astype(numpy.float)
        return df

    def max_open_int_symbol(self,date=None):
        df = self.get_hq_panel(date=date)
        return df['open_int'].idxmax()

    def max_volume_fut(self,date=None):
        max_volume_symbol = self.max_volume_symbol(date=date)
        if max_volume_symbol in self._futures:
            return self._futures[max_volume_symbol]
        return None

    def max_volume_symbol(self,date=None):
        df = self.get_hq_panel(date=date)
        return df['volume'].idxmax()

    def is_max_volume_symbol_changed(self,date=None):
        self.load_hq(date=date)
        max_symbol = self.max_volume_symbol(date=date)
        current_hq_datetime = self.current_hq_datetime()
        dt = current_hq_datetime[max_symbol]

        last_max_symbol = self.max_volume_symbol(date=dt + datetime.timedelta(days=-1))
        last_hq_datetime = self.current_hq_datetime(date=dt + datetime.timedelta(days=-1))
        last_dt = last_hq_datetime[last_max_symbol]

        return last_max_symbol != max_symbol, {'last_dt':last_dt,'current_dt':dt,'max_symbol':max_symbol,'last_max_symbol':last_max_symbol}

    def current_hq_datetime(self,date=None):
        ret = {}
        if date is None:
            date = global_variable.get_now()
        for sym,fut in self._futures.items():
            if not fut.is_trading(date=date):
                continue
            ret[sym] = fut.current_hq_datetime(date=date)
        return ret
    @property
    def current_hq_settle(self):
        ret = {}
        date = global_variable.get_now()
        for sym,fut in self._futures.items():
            if not fut.is_trading(date=date):
                continue
            ret[sym] = fut.current_hq_settle
        return ret
    @property
    def current_hq_close(self):
        ret = {}
        date = global_variable.get_now()
        for sym,fut in self._futures.items():
            if not fut.is_trading(date=date):
                continue
            ret[sym] = fut.current_hq_close
        return ret
    @property
    def current_hq_volume(self):
        ret = {}
        date = global_variable.get_now()
        for sym,fut in self._futures.items():
            if not fut.is_trading(date=date):
                continue
            ret[sym] = fut.current_hq_volume
        return ret

    def upload_info(self,key,value):
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('MARKET')
        cli = db.get_collection('product')
        cli.update_one({'ProductID': self._product_id}, {'$set': {key: value}})

    @staticmethod
    def list_product():
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('MARKET')
        cli = db.get_collection('product')
        result = cli.find({},{'_id':0,'ProductID':1})
        result_list = list(result[:])
        if result_list is None:
            return None
        return [x['ProductID'] for x in result_list]


    def has_night_trading(self,bydate=None):
        ts = self.get_trading_sessions(bydate=bydate)
        tradeNightStartTime = ts.Session4_Start.zfill(8)
        tradeNightEndTime = ts.Session4_End.zfill(8)
        if tradeNightEndTime < '04:00:00' or tradeNightEndTime >= '23:30:00':
            tradeNightEndTime = '23:30:00'
        if tradeNightEndTime != tradeNightStartTime:
            return True
        else:
            return False
    #
    # def _load_info(self):
    #     self._info = DataFactory()._get_instruments()
    #     self._info = self._info .xs(self._asset_type,level='ASSET_TYPE')
    #     if self._symbol is not None:
    #         self._info = self._info.loc[self._symbol]
if __name__ == '__main__':
    from data_engine.instrument.future import Future
    DataFactory.config(MONGDB_PW='jz501241',MONGDB_USER='dbmanager_future',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE,logging_level=global_variable.logging.INFO)
    p_list = Product.list_product()
    for pid in ['SR']:
        p = Product(pid)
        # print(pid,p.list_symbols())
        p.list_futures()
        p.load_hq()
        print(p.is_max_volume_symbol_changed())
        print(p.max_volume_symbol())
        f = p.max_volume_fut()
        if f is not None:
            print(f.ctp_symbol)
        # print(p.product_id, p.max_open_int_symbol(),p.max_volume_symbol(),p.current_hq_datetime)
        # df_1 =p.get_hq_panel(date=global_variable.get_now() + datetime.timedelta(days=-2))
        # df =p.get_hq_panel(date=global_variable.get_now())
        # if df_1['volume'].idxmax() != df['volume'].idxmax():
        #     print(pid,df_1['volume'].idxmax(),df_1['open_int'].idxmax())
        #     print(df_1[['datetime','volume','open_int']])
        #     print(pid,df['volume'].idxmax(),df['open_int'].idxmax())
        #     print(df[['datetime','volume','open_int']])
        # futs = p.get_trading_future(date=global_variable.get_now())
        # for _,each in futs.items():
        #     if isinstance(each,Future):
        #         each.load_hq()
        #         print(each.ctp_symbol,each.current_hq_datetime,each.current_hq_close,each.current_hq_settle,each.current_hq_volume)
                # print(each._hq_df.columns)
                # print(each._symbol, each.ctp_symbol,each.product_id,each._hd_df)
        # p.load_all_future()
        # fut_dict = {}
        # for sym,fut in p._futures.items():
        #     # print(sym,fut._info)
        #     assert isinstance(fut,Future)
        #     if fut.create_date is None:
        #         continue
        #     fut_dict[sym] = fut._info
        # p.upload_info('contract',fut_dict)
        # print(pid)