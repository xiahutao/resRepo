#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/8 16:37
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import datetime
import pandas
import numpy
import data_engine.global_variable as global_variable
from data_engine.instrument.instrument import Instrument
from data_engine.data_factory import DataFactory


class Future(Instrument):
    _product_info = None
    _future_dict = {}
    def __init__(self,symbol,by_ctp_instrument=False,info=None):
        self._the_product_info = None
        self._trading_sessions = None
        Instrument.__init__(self,symbol=symbol,asset_type=global_variable.ASSETTYPE_FUTURE,by_ctp_instrument=by_ctp_instrument,info=info)

        self._hq_df = None

    @property
    def product_class(self):
        if self._info is None:
            return None
        if isinstance(self._info['ProductClass'],str) and self._info['ProductClass'] == 'None':
            return None
        return self._info['ProductClass']
    @staticmethod
    def get_future(symbol,by_ctp_instrument=False):
        if symbol in Future._future_dict:
            return Future._future_dict[symbol]
        else:
            ret = Future(symbol=symbol,by_ctp_instrument=by_ctp_instrument)
            Future._future_dict[symbol] = ret
            return ret

    def load_hq(self,days=-10,end_date=None,freq=global_variable.FREQ_1D):
        if end_date is None:
            end_date = global_variable.get_now()
        start_date = end_date + datetime.timedelta(days=days)
        md = DataFactory.get_future_market_data(freq=freq,symbols=self.jz_symbol,start_date=start_date,end_date=end_date)
        self._hq_df = md[self.jz_symbol]
        if self._hq_df is not None and not self._hq_df.empty:
            for col in ['open','high','close','low','settle','volume','open_int']:
                if col in self._hq_df.columns:
                    self._hq_df[col] = self._hq_df[col].astype(numpy.float)


    def current_hq_datetime(self,date=None):
        if self._hq_df is None:
            return None
        return self._hq_df.iloc[-1].name

    @property
    def ShortMarginRatio(self):
        if self._info is None:
            return None
        if isinstance(self._info['ShortMarginRatio'],str) and self._info['ShortMarginRatio'] == 'None':
            return None
        return float(self._info['ShortMarginRatio'])

    @property
    def LongMarginRatio(self):
        if self._info is None:
            return None
        if isinstance(self._info['LongMarginRatio'],str) and self._info['LongMarginRatio'] == 'None':
            return None
        return float(self._info['LongMarginRatio'])

    @property
    def current_hq_settle(self):
        if self._hq_df is None:
            return None
        return self._hq_df.iloc[-1]['settle']
    @property
    def current_hq_close(self):
        if self._hq_df is None:
            return None
        return self._hq_df.iloc[-1]['close']
    @property
    def current_hq_volume(self):
        if self._hq_df is None:
            return None
        return self._hq_df.iloc[-1]['volume']

    def _load_info(self):
        # self._info = DataFactory()._get_instruments()
        # self._info = self._info .xs(self._asset_type,level='ASSET_TYPE')
        if self._symbol is not None and not self._by_ctp_instrument:
            Instrument._load_info(self)
        elif self._symbol is not None and self._by_ctp_instrument:
            mongo_client = self.mongo_client
            db = mongo_client.get_database('MARKET')
            cli = db.get_collection('instruments')
            self._info = cli.find_one({'ASSET_TYPE': self._asset_type, 'InstrumentID0': self._symbol}, {'_id': 0})
            if self._info is None:
                self._info = cli.find_one({'ASSET_TYPE': self._asset_type, 'InstrumentID0': self._symbol.lower()}, {'_id': 0})
            return self

        if self._product_info is None:
            self._product_info = DataFactory().get_product_info()

        self._the_product_info = self.__get_product_info()
        return self

    def __get_product_info(self):
        product_id = self.product_id
        if product_id is None:
            return None
        product_id = product_id.upper()
        if not product_id in self._product_info.index.unique():
            return None
        return self._product_info.loc[product_id]

    @property
    def sector(self):
        if self._the_product_info is None:
            return None
        if 'Sector' not in self._the_product_info:
            return None
        return self._the_product_info['Sector']

    def _load_trading_sessions(self):
        self._trading_sessions = DataFactory._get_trading_sessions(product_ids=self.product_id.upper())
        return self._trading_sessions

    def get_trading_sessions(self,bydate=None):
        if self._trading_sessions is None:
            self._load_trading_sessions()
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

    def tradeDayStartTime(self,bydate=None):
        ts = self.get_trading_sessions(bydate=bydate)
        tradeDayStartTime = ts.Session1_Start.zfill(8)
        return tradeDayStartTime

    def tradeNightStartTime(self,bydate=None):
        ts = self.get_trading_sessions(bydate=bydate)
        tradeNightStartTime = ts.Session4_Start.zfill(8)
        return tradeNightStartTime

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

if __name__ == '__main__':
    DataFactory.config(MONGDB_PW='jz501241', MONGDB_IP='192.168.2.201', MONGDB_USER='dbmanager_future',
                       DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE
                       , logging_level=global_variable.logging.INFO)
    f = Future(symbol='TF_VOL')
    print(f.contract_size)
    print(f.product_id)
    print(f.sector)
    print(f._load_trading_sessions())
    ts = f.get_trading_sessions()
    print(ts)

    # import datetime
    # print(datetime.time(ts['Session4_Start']))