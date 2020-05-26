# -*- coding: utf-8 -*-
import os
import pickle
import logging
import pandas
import numpy
import datetime
import pymongo
import json

from functools import lru_cache,partial
from arctic import Arctic
from data_engine.data.market_data import MarketData

from common.decorator import runing_time
import data_engine.setting as Setting
import data_engine.global_variable as global_variable
from common.mult_threading import MyThread
# from data_engine.setting import DATASOURCE_REMOTE,DATASOURCE_LOCAL
# from data_engine.setting import ASSETTYPE_STOCK,ASSETTYPE_FUTURE,ASSETTYPE_BOND,ASSETTYPE_INDEX,ASSETTYPE_ETF,ASSETTYPE_INDEX_WEIGHT
# from data_engine.setting import FREQ_TICK,FREQ_1M,FREQ_1D,FREQ_5M
# from data_engine.setting import STOCK_TICK_LIB,STOCK_DAILY_LIB,STOCK_MINUTE_LIB
# from data_engine.setting import BOND_DAILY_LIB,BOND_MINUTE_LIB,BOND_TICK_LIB
# from data_engine.setting import FUTURE_TICK_LIB,FUTURE_DAILY_LIB,FUTURE_MINUTE_LIB,FUTURE_MINUTE_5_LIB
# from data_engine.setting import EXCHANGE_ID_CZC,EXCHANGE_ID_DCE,EXCHANGE_ID_SHF
# from data_engine.setting import INDEX_WEIGHT_LIB

from data_engine.data.file_data.file_data import FileData
from common.singleton import Singleton
from common.file_saver import file_saver

class DataFactory(object,metaclass=Singleton):
    '''
     数据接口工具类
    '''

    _logger = logging.getLogger(__name__)

    def __init__(self, dataSource=None):
        '''
        :param dataSource: 指定使用本地数据文件，或远程数据库
        '''
        self._mongo_client = None
        self._assettype_freq_marketdataclass_dict = {}
        self._assettype_freq_marketdataobject_dict={}

        self._instruments_df_dict = {}

        self._market_date_dict = {}
        if dataSource is None:
            dataSource = Setting.DATASOURCE_DEFAULT

        if dataSource == global_variable.DATASOURCE_REMOTE:
            from data_engine.data.arctic_data.future_daily_data import FutureDailyData
            # from data_engine.data.mssql_data.future_daily_data import FutureDailyData
            # from data_engine.data.mssql_data.future_minute_data import FutureMinuteData
            from data_engine.data.arctic_data.future_minute_data import FutureMinuteData,Future5MinuteData
            from data_engine.data.arctic_data.stock_tick_data import StockTickData
            from data_engine.data.arctic_data.future_tick_data import FutureTickData
            from data_engine.data.arctic_data.stock_daily_data import StockDailyData
            from data_engine.data.arctic_data.bond_daily_data import BondDailyData

            from data_engine.data.arctic_data.index_weight_daily_data import IndexWeightDailyData
            from data_engine.data.arctic_data.stock_feature_daily_data import StockFeatureDailyData

            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_FUTURE, global_variable.FREQ_1D)] = FutureDailyData
            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_FUTURE, global_variable.FREQ_1M)] = FutureMinuteData
            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_FUTURE, global_variable.FREQ_5M)] = Future5MinuteData
            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_FUTURE, global_variable.FREQ_TICK)] = FutureTickData
            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_BOND, global_variable.FREQ_1D)] = BondDailyData
            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_INDEX_WEIGHT, global_variable.FREQ_1D)] = IndexWeightDailyData

            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_STOCK, global_variable.FREQ_TICK)] = StockTickData
            self._assettype_freq_marketdataclass_dict[(global_variable.ASSETTYPE_STOCK, global_variable.FREQ_1D)] = StockDailyData

            self._assettype_freq_marketdataclass_dict[(Setting.ASSETTYPE_STOCK_FEATURE, global_variable.FREQ_1D)] = StockFeatureDailyData
        elif dataSource == global_variable.DATASOURCE_LOCAL:
            from data_engine.data.file_data.file_daily_data import FileDailyData
            from data_engine.data.file_data.file_minute_data import FileMinuteData,File5MinuteData
            from data_engine.data.file_data.file_tick_data import FileTickData

            for asset_type in [global_variable.ASSETTYPE_STOCK,global_variable.ASSETTYPE_BOND,global_variable.ASSETTYPE_FUTURE]:
                self._assettype_freq_marketdataclass_dict[(asset_type, global_variable.FREQ_1D)] = FileDailyData
                self._assettype_freq_marketdataclass_dict[(asset_type, global_variable.FREQ_1M)] = FileMinuteData
                self._assettype_freq_marketdataclass_dict[(asset_type, global_variable.FREQ_5M)] = File5MinuteData
                self._assettype_freq_marketdataclass_dict[(asset_type, global_variable.FREQ_TICK)] = FileTickData

    def __del__(self):
        if self._mongo_client is not None:
            self._mongo_client.close()

    @property
    def mongo_client(self):
        return self._mongo_client

    @mongo_client.setter
    def mongo_client(self,value):
        self._mongo_client = value

    @staticmethod
    def config(MONGDB_PW,MONGDB_IP=None,MONGDB_USER='juzheng'
               ,DATASOURCE_DEFAULT=global_variable.DATASOURCE_LOCAL
               ,DAILY_FILE_FOLDER=None
               ,MINUTE_FILE_SOURCE=None
               ,MINUTE_FILE_SOURCE_5M=None
               ,TICK_FILE_SOURCE=None
               ,logging_level = global_variable.logging.DEBUG
               ,temp_path= None):
        if MONGDB_PW is None or MONGDB_PW == '':
            print('No password of mongodb!!')
            # raise 0
        if MONGDB_IP is None:
            pass
        elif isinstance(MONGDB_IP,list):
            Setting.MONGDB_IP_list = MONGDB_IP
        else:
            Setting.MONGDB_IP = MONGDB_IP
        Setting.MONGDB_USER = MONGDB_USER
        Setting.MONGDB_PW = MONGDB_PW
        Setting.DATASOURCE_DEFAULT = DATASOURCE_DEFAULT
        if DAILY_FILE_FOLDER is not None:
            Setting.DAILY_FILE_FOLDER = DAILY_FILE_FOLDER
        if MINUTE_FILE_SOURCE is not None:
            Setting.MINUTE_FILE_SOURCE = MINUTE_FILE_SOURCE
        if MINUTE_FILE_SOURCE_5M is not None:
            Setting.MINUTE_FILE_SOURCE_5M = MINUTE_FILE_SOURCE_5M
        if TICK_FILE_SOURCE is not None:
            Setting.TICK_FILE_SOURCE = TICK_FILE_SOURCE

        Setting.logging_level = logging_level
        Setting.TEMP = temp_path

    @staticmethod
    def _assettype_freq_arcticlib(asset_type,freq):
        if (asset_type,freq) == (global_variable.ASSETTYPE_FUTURE,global_variable.FREQ_TICK):
            return global_variable.FUTURE_TICK_LIB
        elif (asset_type,freq) == (global_variable.ASSETTYPE_FUTURE,global_variable.FREQ_1D):
            return global_variable.FUTURE_DAILY_LIB
        elif (asset_type,freq) == (global_variable.ASSETTYPE_FUTURE,global_variable.FREQ_1M):
            return global_variable.FUTURE_MINUTE_LIB
        elif (asset_type,freq) == (global_variable.ASSETTYPE_FUTURE,global_variable.FREQ_5M):
            return global_variable.FUTURE_MINUTE_5_LIB

        if (asset_type,freq) == (global_variable.ASSETTYPE_BOND,global_variable.FREQ_TICK):
            return global_variable.BOND_TICK_LIB
        elif (asset_type,freq) == (global_variable.ASSETTYPE_BOND,global_variable.FREQ_1D):
            return global_variable.BOND_DAILY_LIB
        elif (asset_type,freq) == (global_variable.ASSETTYPE_BOND,global_variable.FREQ_1M):
            return global_variable.BOND_MINUTE_LIB

        if (asset_type,freq) == (global_variable.ASSETTYPE_STOCK,global_variable.FREQ_TICK):
            return global_variable.STOCK_TICK_LIB
        elif (asset_type,freq) == (global_variable.ASSETTYPE_STOCK,global_variable.FREQ_1D):
            return global_variable.STOCK_DAILY_LIB
        elif (asset_type,freq) == (global_variable.ASSETTYPE_STOCK,global_variable.FREQ_1M):
            return global_variable.STOCK_MINUTE_LIB

        elif (asset_type,freq) == (Setting.ASSETTYPE_STOCK_FEATURE,global_variable.FREQ_1D):
            return Setting.FEATURE_DAILY_LIB

        if (asset_type,freq) == (global_variable.ASSETTYPE_INDEX_WEIGHT,global_variable.FREQ_1D):
            return global_variable.INDEX_WEIGHT_LIB

        return None

    @staticmethod
    def get_mongo_client():
        """
        获取Mongo库客户端对象
        :return:
        """
        if DataFactory().mongo_client is None:
            DataFactory().mongo_client = MarketData.get_mongo_client()
            return DataFactory().mongo_client
        else:
            return DataFactory().mongo_client

    @staticmethod
    def _check_symbol_need_reload(data_df,symbol,asset_type,freq):
        metadata = None
        if data_df is None or data_df.empty:
            return True
        metadata = {'l': len(data_df), 's': data_df.iloc[0].name, 'e': data_df.iloc[-1].name}

        metadata_remote = DataFactory.load_metadata(symbol=symbol, asset_type=asset_type, freq=freq)

        need_sync = False
        if metadata_remote is None:
            need_sync = True
        # if metadata_remote is None\
        #         or 'l' not in metadata_remote\
        #         or 's' not in metadata_remote\
        #         or 'e' not in metadata_remote:
        #     return False
        if metadata_remote is not None:
            if metadata is None or \
                    metadata_remote['l'] == metadata['l'] \
                    and metadata_remote['s'] == metadata['s'] \
                    and metadata_remote['e'] == metadata['e']:
                need_sync = True
        return need_sync

    @staticmethod
    def sync_stock_from_remote(symbol,freq,start_date=None,end_date=None,price_type=Setting.PRICE_TYPE_UN):
        return DataFactory.sync_from_remote(asset_type=Setting.ASSETTYPE_STOCK,symbol=symbol,
                                            freq=freq,start_date=start_date,end_date=end_date,
                                            price_type=price_type)


    @staticmethod
    def sync_future_by_product_id(product_id,freq,start_date=None,end_date=None):
        inst = DataFactory.get_instruments_byproductID(productID=product_id)
        inst.reset_index(inplace=True)
        DataFactory.sync_future_from_remote(symbol=list(inst['symbol']), freq=freq,start_date=start_date,end_date=end_date)
        
        DataFactory.sync_future_from_remote(symbol=[product_id,product_id+'_VOL',product_id+'_S',product_id+'_S_VOL'], freq=freq,start_date=start_date,end_date=end_date)


    @staticmethod
    def sync_future_from_remote(symbol,freq,start_date=None,end_date=None):
        return DataFactory.sync_from_remote(asset_type=Setting.ASSETTYPE_FUTURE,symbol=symbol,
                                            freq=freq,start_date=start_date,end_date=end_date,
                                            price_type=Setting.PRICE_TYPE_UN)
    @staticmethod
    def sync_from_remote(asset_type,symbol,freq,start_date=None,end_date=None,price_type=Setting.PRICE_TYPE_UN):
        """
        从数据库服务器同步指定频率下，指定合约的时序数据到本地文件
        :param asset_type:
        :param symbol:
        :param freq:
        :return:
        """
        print(('sync_from_remote',symbol))
        libstr = DataFactory._assettype_freq_arcticlib(asset_type=asset_type,freq=freq)
        if libstr is None:
            return None

        filedata = FileData(freq=freq)

        symbol_need_sync = []
        if filedata.load_market_data(symbols=symbol,freq=freq,start_date=start_date,end_date=end_date,price_type=price_type):
            if isinstance(symbol,str):
                if not filedata.has_data((symbol,freq,price_type)):
                    symbol_need_sync.append(symbol)
                else:
                    data_df = filedata[(symbol,freq,price_type)]
                    if DataFactory._check_symbol_need_reload(data_df=data_df,symbol=symbol,asset_type=asset_type,freq=freq):
                        symbol_need_sync.append(symbol)
            elif isinstance(symbol,list):
                for symbol_tmp in symbol:
                    if not filedata.has_data((symbol_tmp,freq,price_type)):
                        symbol_need_sync.append(symbol_tmp)
                    else:
                        data_df = filedata[(symbol_tmp,freq,price_type)]
                        if DataFactory._check_symbol_need_reload(data_df=data_df,symbol=symbol_tmp,asset_type=asset_type,freq=freq):
                            symbol_need_sync.append(symbol_tmp)

        if len(symbol_need_sync)>0:
            if libstr == global_variable.FUTURE_TICK_LIB:
                from data_engine.data.arctic_data.future_tick_data import FutureTickData
                marketdata_class = FutureTickData
            elif libstr == global_variable.FUTURE_DAILY_LIB:
                from data_engine.data.arctic_data.future_daily_data import FutureDailyData
                marketdata_class = FutureDailyData
            elif libstr == global_variable.FUTURE_MINUTE_LIB:
                from data_engine.data.arctic_data.future_minute_data import FutureMinuteData
                marketdata_class = FutureMinuteData
            elif libstr == global_variable.FUTURE_MINUTE_5_LIB:
                from data_engine.data.arctic_data.future_minute_data import Future5MinuteData
                marketdata_class = Future5MinuteData
            elif libstr == global_variable.INDEX_WEIGHT_LIB:
                from data_engine.data.arctic_data.index_weight_daily_data import IndexWeightDailyData
                marketdata_class = IndexWeightDailyData

            elif libstr == global_variable.BOND_DAILY_LIB:
                from data_engine.data.arctic_data.bond_daily_data import BondDailyData
                marketdata_class = BondDailyData

            elif libstr == global_variable.STOCK_TICK_LIB:
                from data_engine.data.arctic_data.stock_tick_data import StockTickData
                marketdata_class = StockTickData
            elif libstr == global_variable.STOCK_DAILY_LIB:
                from data_engine.data.arctic_data.stock_daily_data import StockDailyData
                marketdata_class = StockDailyData
            else:
                return None

            marketdata = marketdata_class(price_type=price_type).load_market_data(symbol_need_sync,start_date=start_date,end_date=end_date,price_type=price_type)
            marketdata.SaveToLocal_File(symbol_need_sync,price_type)
            del marketdata

            folder = Setting.DAILY_FILE_FOLDER
            for col_name in ['instruments','instruments_jq']:
                instrument_info = DataFactory._get_instruments(collection=col_name)
                file = os.path.join(folder, col_name + '_info.pkl')
                print(('SaveToLocal_File', file))
                with open(file, 'wb') as handle:
                    pickle.dump(instrument_info, handle)

            return True

        return False

    @staticmethod
    def to_timestamp(date,tz=Setting.DEFAULT_TIMEZONE):
        return MarketData.to_timestamp(date,tz=tz)

    @staticmethod
    def load_metadata(symbol,asset_type,freq):
        """
        获取指定频率下，指定合约的metadata(arctic)
        :param symbol:
        :param asset_type:
        :param freq:
        :return:
        """
        libstr = DataFactory._assettype_freq_arcticlib(asset_type=asset_type,freq=freq)

        mongoclient = DataFactory.get_mongo_client()
        arc = Arctic(mongoclient)
        lib = arc.get_library(libstr)

        metadata = None
        try:
            metadata = lib.read_metadata(symbol=symbol)
        except:
            pass
        return metadata

    @staticmethod
    def get_product_info():

        mongoclient = DataFactory.get_mongo_client()
        db = mongoclient.get_database('MARKET')
        cl = db.get_collection('product')
        query = {}
        results = cl.find(query)
        results_list = [x for x in results]
        result_df = pandas.DataFrame(results_list)
        if not result_df.empty:
            result_df = result_df.set_index('ProductID')
        return result_df


       #  instrument_info = DataFactory._get_instruments(asset_type=global_variable.ASSETTYPE_FUTURE)
       #  instrument_info = instrument_info[(instrument_info['ProductClass'] == ProductClass) & (instrument_info['CreateDate'].notna())]
       #  instrument_info['ProductID'] = instrument_info['ProductID'].str.upper()
       #  instrument_info.sort_values('CreateDate',inplace=True)
       #  product_info = instrument_info.groupby('ProductID').last()
       #  product_info.drop(columns=['CombinationType','CreateDate', 'DLMONTH', 'DeliveryMonth', 'DeliveryYear',
       # 'EndDelivDate', 'ExchangeInstID', 'ExpireDate',
       # 'InstLifePhase', 'InstrumentCode', 'InstrumentID0', 'InstrumentName',
       # 'IsTrading', 'MarkAsMainContract', 'MarkIt',
       # 'OpenDate', 'OptionsType', 'PositionDateType',
       # 'PositionType', 'ProductClass',
       # 'StartDelivDate', 'StrikePrice', 'UnderlyingInstrID',
       # 'UnderlyingMultiple', '_id', 'main_contract_from',
       # 'main_contract_to'],inplace=True)
       #  return product_info

    # @lru_cache()
    def __get_instruments(self,collection='instruments'):
        if Setting.DATASOURCE_DEFAULT == global_variable.DATASOURCE_LOCAL:
            folder = Setting.DAILY_FILE_FOLDER
            file = os.path.join(folder, collection + '_info.pkl')
            if os.path.exists(file):
                with open(file,'rb') as handle:
                    return_df = pickle.load(handle)
                    if not return_df is None and not return_df.empty:
                        return_df['PriceTick'] = return_df['PriceTick'].astype(numpy.float)
                        return_df['VolumeMultiple'] = return_df['VolumeMultiple'].astype(numpy.float)
                        return_df['ProductID'] = return_df['ProductID'].str.upper()
                        if 'ShortMarginRatio' not in return_df.columns:
                            return_df['ShortMarginRatio'] = 0.1
                        return return_df
        mongoclient = DataFactory.get_mongo_client()
        db = mongoclient.get_database('MARKET')
        instruments = db.get_collection(collection)
        query = {}
        results = instruments.find(query)
        results_list = [x for x in results]
        return_df = pandas.DataFrame(results_list)
        if not return_df.empty:
            if 'ASSET_TYPE' in return_df.columns:
                return_df.loc[return_df['ASSET_TYPE']==Setting.ASSETTYPE_STOCK,'PriceTick'] = 0.01
                return_df.loc[return_df['ASSET_TYPE']==Setting.ASSETTYPE_STOCK,'ProductID'] = 'stock'
                return_df.loc[return_df['ASSET_TYPE']==Setting.ASSETTYPE_STOCK,'VolumeMultiple'] = 100
            return_df['PriceTick'] = return_df['PriceTick'].astype(numpy.float)
            return_df['VolumeMultiple'] = return_df['VolumeMultiple'].astype(numpy.float)
            return_df['ProductID'] = return_df['ProductID'].str.upper()
            if 'ShortMarginRatio' not in return_df.columns:
                return_df['ShortMarginRatio'] = 0.1
            if 'ASSET_TYPE' in return_df.columns and 'symbol' in return_df.columns:
                return_df.set_index(['ASSET_TYPE', 'symbol'], inplace=True)
            elif 'symbol' in return_df.columns:
                return_df.set_index(['symbol'], inplace=True)
        return return_df

    @staticmethod
    @runing_time
    def get_strategy_log(strategy_type):
        client = DataFactory.get_mongo_client()
        db = client.get_database('strategy_log')
        cl = db.get_collection(strategy_type)
        result = cl.find()
        result_list = list(result[:])
        return result_list

    @staticmethod
    @runing_time
    def _get_trading_sessions(product_ids=None):
        mongoclient = DataFactory.get_mongo_client()
        db = mongoclient.get_database('MARKET')
        TradingSessions = db.get_collection('TradingSessions')
        query = {}
        results = TradingSessions.find(query)
        return_df = pandas.DataFrame(results[:])
        return_df.set_index('Market',inplace=True)
        return_df['DateRange_Start'].fillna(datetime.date(1990,1,1),inplace=True)
        return_df['DateRange_End'].fillna(datetime.date(2199,1,1),inplace=True)

        return_df['DateRange_Start'] = pandas.to_datetime(return_df['DateRange_Start'])
        return_df['DateRange_End'] = pandas.to_datetime(return_df['DateRange_End'])
        return_df['Session4_Start'].fillna('21:00:00',inplace=True)
        return_df['Session4_End'].fillna('21:00:00',inplace=True)

        # for col in ['Session1_Start']:
        #     return_df[col] = pandas.to_datetime(return_df[col],format='%H:%M:%S', errors='ignore')
        if not return_df.empty:
            if product_ids is not None:
                return return_df.loc[product_ids]
        return return_df

    @staticmethod
    def _get_instruments(symbols=None, asset_type=None,collection='instruments'):
        """
        获取合约信息
        :param symbols:
        :param asset_type:
        :return:
        """

        return_df = DataFactory().__get_instruments(collection=collection)
        return return_df

    @staticmethod
    def get_create_date_dict(symbols=None, asset_type=Setting.ASSETTYPE_FUTURE):
        if isinstance(symbols,str):
            symbols_tmp = [symbols]
        elif isinstance(symbols,list):
            symbols_tmp = symbols
        else:
            return None

        # if asset_type == Setting.ASSETTYPE_STOCK:
        #     if isinstance(symbols,str):
        #         return 0.01
        #     elif isinstance(symbols,list):
        #         return { x: 0.01 for x in symbols}
        #     else:
        #         return None

        market_info_df = DataFactory._get_instruments(symbols=symbols_tmp,
                                                     asset_type=asset_type)
        if isinstance(symbols,str):
            return market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbols.upper()), 'CreateDate']
        ret = {}
        for symbol in symbols:
            ret[symbol] = market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbol.upper()), 'CreateDate']
        return ret

    @staticmethod
    def get_expire_date_dict(symbols=None, asset_type=global_variable.ASSETTYPE_FUTURE):
        if isinstance(symbols,str):
            symbols_tmp = [symbols]
        elif isinstance(symbols,list):
            symbols_tmp = symbols
        else:
            return None

        # if asset_type == global_variable.ASSETTYPE_STOCK:
        #     if isinstance(symbols,str):
        #         return 0.01
        #     elif isinstance(symbols,list):
        #         return { x: 0.01 for x in symbols}
        #     else:
        #         return None

        market_info_df = DataFactory._get_instruments(symbols=symbols_tmp,
                                                     asset_type=asset_type)
        if isinstance(symbols,str):
            return market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbols.upper()), 'ExpireDate']
        ret = {}
        for symbol in symbols:
            ret[symbol] = market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbol.upper()), 'ExpireDate']
        return ret

    @staticmethod
    def get_tick_size_dict(symbols=None, asset_type=global_variable.ASSETTYPE_FUTURE):
        if isinstance(symbols,str):
            symbols_tmp = [symbols]
        elif isinstance(symbols,list):
            symbols_tmp = symbols
        else:
            return None

        if asset_type == Setting.ASSETTYPE_STOCK:
            if isinstance(symbols,str):
                return 0.01
            elif isinstance(symbols,list):
                return { x: 0.01 for x in symbols}
            else:
                return None

        market_info_df = DataFactory._get_instruments(symbols=symbols_tmp,
                                                     asset_type=asset_type)
        if isinstance(symbols,str):
            return float(market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbols.upper()), 'PriceTick'])
        ret = {}
        for symbol in symbols:
            ret[symbol] = float(market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbol.upper()), 'PriceTick'])

        return ret

    @staticmethod
    def get_market_dict(symbols=None, asset_type=global_variable.ASSETTYPE_FUTURE):
        if isinstance(symbols,str):
            symbols_tmp = [symbols]
        elif isinstance(symbols,list):
            symbols_tmp = symbols
        else:
            return None

        # if asset_type == Setting.ASSETTYPE_STOCK:
        #     if isinstance(symbols,str):
        #         return 0.01
        #     elif isinstance(symbols,list):
        #         return { x: 0.01 for x in symbols}
        #     else:
        #         return None

        market_info_df = DataFactory._get_instruments(symbols=symbols_tmp,
                                                     asset_type=asset_type)
        if isinstance(symbols,str):
            row = market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbols.upper())]
            return str(row['ExchangeID'])
        ret = {}
        for symbol in symbols:
            row = market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbol.upper())]
            ret[symbol] = str(row['ExchangeID'])

        return ret

    @staticmethod
    def get_ctp_symbol_dict(symbols=None, asset_type=global_variable.ASSETTYPE_FUTURE):
        if isinstance(symbols,str):
            symbols_tmp = [symbols]
        elif isinstance(symbols,list):
            symbols_tmp = symbols
        else:
            return None

        # if asset_type == Setting.ASSETTYPE_STOCK:
        #     if isinstance(symbols,str):
        #         return 0.01
        #     elif isinstance(symbols,list):
        #         return { x: 0.01 for x in symbols}
        #     else:
        #         return None

        market_info_df = DataFactory._get_instruments(symbols=symbols_tmp,
                                                     asset_type=asset_type)
        if isinstance(symbols,str):
            return str(market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbols.upper()), 'InstrumentID0'])
        ret = {}
        for symbol in symbols:
            ret[symbol] = str(market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbol.upper()), 'InstrumentID0'])

        return ret

    @staticmethod
    def get_product_id_dict(symbols=None, asset_type=global_variable.ASSETTYPE_FUTURE):
        if isinstance(symbols,str):
            symbols_tmp = [symbols]
        elif isinstance(symbols,list):
            symbols_tmp = symbols
        else:
            return None

        if asset_type == Setting.ASSETTYPE_STOCK:
            if isinstance(symbols,str):
                return 'stock'
            elif isinstance(symbols,list):
                return { x: 'stock' for x in symbols}
            else:
                return None
        market_info_df = DataFactory._get_instruments(symbols=symbols_tmp,
                                                     asset_type=asset_type)
        if isinstance(symbols,str):
            return str(market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbols.upper()), 'ProductID']).upper()

        ret = {}
        for symbol in symbols_tmp:
            ret[symbol] = str(market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbol.upper()), 'ProductID']).upper()
        return ret

    @staticmethod
    def get_contract_size_dict(symbols=None, asset_type=global_variable.ASSETTYPE_FUTURE):
        if isinstance(symbols,str):
            symbols_tmp = [symbols]
        elif isinstance(symbols,list):
            symbols_tmp = symbols
        else:
            return None

        if asset_type == global_variable.ASSETTYPE_STOCK:
            if isinstance(symbols,str):
                return 100
            elif isinstance(symbols,list):
                return { x: 100 for x in symbols}
            else:
                return None

        market_info_df = DataFactory._get_instruments(symbols=symbols_tmp,
                                                     asset_type=asset_type)
        if isinstance(symbols,str):
            return float(market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbols.upper()), 'VolumeMultiple'])
        elif isinstance(symbols,list):
            ret = {}
            for symbol in symbols:
                value= market_info_df.loc[(global_variable.ASSETTYPE_FUTURE, symbol.upper()), 'VolumeMultiple']
                ret[symbol] = float(value)

            return ret
        return None

    @staticmethod
    def get_stock_symbols(collection='instruments'):
        stock_info = DataFactory.get_stock_info(collection=collection)
        stock_info.reset_index(inplace=True)
        symbols = stock_info['symbol'].unique()
        return list(symbols)

    @staticmethod
    def get_stock_info(collection='instruments'):
        stocks_info = DataFactory._get_instruments(asset_type=global_variable.ASSETTYPE_STOCK,collection=collection)
        return stocks_info

    @staticmethod
    def get_etf_info(collection='instruments_jq'):
        ETF_info = DataFactory._get_instruments(asset_type=global_variable.ASSETTYPE_ETF,collection=collection)
        return ETF_info

    @staticmethod
    def get_index_info(collection='instruments_jq'):
        index_info = DataFactory._get_instruments(asset_type=global_variable.ASSETTYPE_INDEX,collection=collection)
        return index_info

    @staticmethod
    def get_industry_info(type='sw_l1',collection='industries_jq'):
        industry_info = DataFactory._get_instruments(collection=collection)
        if type is not None:
            industry_info = industry_info[industry_info['type'] == type]
        return industry_info

    @staticmethod
    def get_industry_stocks(type='sw_l1'):
        mongoclient = DataFactory.get_mongo_client()
        db = mongoclient.get_database('MARKET')

        query = {}
        if type is not None:
            query['type'] = type
        collection = db.get_collection('industry_stocks_jq')
        industry_stocks_jq = collection.find(query)
        industry_stocks_jq_df = pandas.DataFrame(industry_stocks_jq[:])

        return industry_stocks_jq_df


    @staticmethod
    def get_instruments_byproductID(
            productID,
            create_before=None, create_after=None,
            expire_before=None, expire_after=None,
            onlycontract=True
    ):
        """
        获取同品种/市场下的合约信息
        :param productID:  品种代码
        :param create_before:  截止指定日之前上市合约
        :param create_after:  截止指定日之后上市合约
        :param expire_before: 截止指定日之前到期合约
        :param expire_after:  截止指定日之后到期合约
        :param onlycontract:  True,只获取主力合约；False,获取全部合约
        :return:
        """
        mongoclient = DataFactory.get_mongo_client()
        db = mongoclient.get_database('MARKET')
        instruments = db.get_collection('instruments')

        query = {}
        if productID is not None:
            query['$or'] = [
                {'ProductID': productID.upper()},
                {'ProductID': productID.lower()},
                {'ProductID': productID}
            ]

        result = instruments.find(query)
        return_df = pandas.DataFrame(result[:])
        if not return_df.empty:
            return_df.set_index(['ASSET_TYPE', 'symbol'], inplace=True)
            if onlycontract:
                return_df = return_df[(return_df['CreateDate'].notna()) & (return_df['CreateDate'] != 'None')]
            for col in ['CreateDate', 'ExpireDate']:
                return_df[col] = pandas.to_datetime(return_df[col])
            return_df.sort_values('CreateDate', inplace=True)

            if create_before is not None:
                return_df = return_df[return_df['CreateDate'] <= pandas.to_datetime(create_before)]
            if create_after is not None:
                return_df = return_df[return_df['CreateDate'] >= pandas.to_datetime(create_after)]

            if expire_before is not None:
                return_df = return_df[return_df['ExpireDate'] <= pandas.to_datetime(expire_before)]
            if expire_after is not None:
                return_df = return_df[return_df['ExpireDate'] >= pandas.to_datetime(expire_after)]
            return return_df
        return None

    def get_feature_data(self, asset_type, freq, symbols, start_date, end_date,version, **kwargs):
        pass

    def get_industry_stocks_map(self):
        pass

    @staticmethod
    def get_future_market_data(freq, symbols, start_date, end_date, **kwargs):
        return DataFactory.get_market_data(asset_type=global_variable.ASSETTYPE_FUTURE,freq=freq,
                                    symbols=symbols,start_date=start_date,end_date=end_date,**kwargs)

    @staticmethod
    def get_stock_market_data(freq, symbols, start_date, end_date,price_type=Setting.PRICE_TYPE_UN, **kwargs):
        return DataFactory.get_market_data(asset_type=global_variable.ASSETTYPE_STOCK,freq=freq,
                                    symbols=symbols,start_date=start_date,end_date=end_date,
                                    price_type=price_type,**kwargs)

    def get_stock_feature_market_data(self,freq, symbols, start_date, end_date,price_type=Setting.PRICE_TYPE_UN, **kwargs):
        return self.get_market_data(asset_type=global_variable.ASSETTYPE_STOCK_FEATURE,freq=freq,
                                    symbols=symbols,start_date=start_date,end_date=end_date,
                                    price_type=price_type,**kwargs)

    @staticmethod
    def clear_data():
        DataFactory()._assettype_freq_marketdataobject_dict = {}

    @staticmethod
    @runing_time
    def get_market_data(asset_type, freq, symbols, start_date=None, end_date=None,price_type=Setting.PRICE_TYPE_UN, **kwargs):
        '''
        获取指定合约，指定频率下的时序数据
        :param asset_type: future
        :param freq: 1d 1m
        :param symbols:
        :param start_date:
        :param end_date:
        :param kwargs:
        :return:
        '''
        if isinstance(symbols,str):
            symbols = [symbols]
        datafactory = DataFactory()
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
        market_data = None
        if (asset_type, freq,price_type) in datafactory._assettype_freq_marketdataobject_dict:
            market_data = datafactory._assettype_freq_marketdataobject_dict[(asset_type, freq,price_type)]
            market_data.load_market_data(symbols, start_date, end_date, **kwargs)

        elif (asset_type, freq) in datafactory._assettype_freq_marketdataclass_dict:
            marketdata_class = datafactory._assettype_freq_marketdataclass_dict[(asset_type, freq)]
            market_data = marketdata_class(price_type=price_type).load_market_data(symbols, start_date, end_date, **kwargs)
            datafactory._assettype_freq_marketdataobject_dict[(asset_type, freq,price_type)] = market_data

        else:
            datafactory._logger.error("No data interface class for (%s,%s,%s):" % (asset_type, freq,price_type))
            pass
        return market_data

    @staticmethod
    def get_dataframe_dict_bycolumn(dataframe_obj,column):
        if dataframe_obj is None:
            return None
        if column not in dataframe_obj.columns:
            return None
        ret_dict = {}
        for key,sub_dataframe in dataframe_obj.groupby(column):
            ret_dict[key] = sub_dataframe
        return ret_dict

    @staticmethod
    def get_dataframe_bycolumn(key_dataframe_dict,column,join='inner'):
        if key_dataframe_dict is None:
            return None
        col_series_list = []
        df=None
        for symbol, data_df in key_dataframe_dict.items():
            if column not in data_df.columns:
                continue
            ret = data_df[column]
            if ret is not None:
                ret.name = symbol
                col_series_list.append(ret)

        if len(col_series_list)>0:
            col_dataframe = pandas.concat(col_series_list,axis=1,join=join)
            col_dataframe.index.name = column
            return col_dataframe
        return None

    @staticmethod
    def get_dataframe_pivot(dataframe_obj,key_column,value_column):
        if dataframe_obj is None:
            return None
        if key_column not in dataframe_obj.columns or value_column not in dataframe_obj.columns:
            return None
        return dataframe_obj.pivot(columns=key_column,values=value_column)

    @staticmethod
    @runing_time
    def get_trading_date_str_series(data_df,isdaily=False):
        ret_df = data_df[[data_df.columns[0]]].copy()

        ret_df['actiondate'] = ret_df.index.date
        ret_df['trade_date_0'] = (ret_df.index + datetime.timedelta(hours=3,minutes=1)).date
        tradingdate = DataFactory().get_trading_date(start_date=ret_df.index.min(),end_date=ret_df.index.max())
        # tradingdate = tradingdate.loc[tradingdate['isTradingday'],]
        ret_df_ex = pandas.merge(ret_df,tradingdate,how='left',left_on='actiondate',right_on='actiondate')
        ret_df_ex.index = ret_df.index
        if not isdaily:
            ret_df_ex['trade_date'] = numpy.where(ret_df_ex['trade_date_0'] == ret_df_ex['actiondate'],ret_df_ex['Tradedays'],ret_df_ex['next_trading_date'])
            ret_df_ex['next_trading_date'] = numpy.where(ret_df_ex['trade_date_0'] == ret_df_ex['actiondate'],ret_df_ex['next_trading_date'],ret_df_ex['next_trading_date_2'])
            ret_df_ex['last_trading_date'] = numpy.where(ret_df_ex['trade_date_0'] == ret_df_ex['actiondate'],ret_df_ex['last_trading_date'],ret_df_ex['Tradedays'])
        else:
            ret_df_ex['trade_date'] = ret_df_ex['Tradedays']
            ret_df_ex['next_trading_date'] = ret_df_ex['trade_date'].shift(-1)
            ret_df_ex['last_trading_date'] = ret_df_ex['trade_date'].shift(1)

        return (ret_df_ex['trade_date'].dt.date,ret_df_ex['last_trading_date'].dt.date,ret_df_ex['next_trading_date'].dt.date)

    @runing_time
    @lru_cache()
    def get_trading_date(self,start_date=None, end_date=None,exchangeid = global_variable.EXCHANGE_ID_SHF):
        query = {}
        # if start_date is not None:
        #     if 'Tradedays' not in query:
        #         query['Tradedays'] = {}
        #     query['Tradedays']['$gt'] = pandas.to_datetime(start_date).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) + datetime.timedelta(days=-15)
        # if end_date is not None:
        #     if 'Tradedays' not in query:
        #         query['Tradedays'] = {}
        #     query['Tradedays']['$lt'] = pandas.to_datetime(end_date).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None) + datetime.timedelta(days=15)

        if exchangeid in self._market_date_dict:
            result_df = self._market_date_dict[exchangeid].copy()
            if start_date is not None:
                result_df = result_df.loc[
                                pandas.to_datetime(start_date).replace(hour=0, minute=0, second=0, microsecond=0,
                                                                       tzinfo=None):]
            if end_date is not None:
                result_df = result_df.loc[
                                :pandas.to_datetime(end_date).replace(hour=23, minute=59, second=59, microsecond=0,
                                                                          tzinfo=None)]
            return result_df

        mongoclient = DataFactory.get_mongo_client()
        db = mongoclient.get_database('Tradedays')
        if exchangeid.upper() == 'DCE':
            collection = db.get_collection(global_variable.EXCHANGE_ID_DCE)
        elif exchangeid.upper() == 'CZCE':
            collection = db.get_collection(global_variable.EXCHANGE_ID_CZC)
        else:
            collection = db.get_collection(global_variable.EXCHANGE_ID_SHF)
        data = collection.find(query)
        result_list = list(data[:])
        if len(result_list) > 0:
            result_df = pandas.DataFrame(result_list)
            result_df = result_df.drop(columns=['_id'])
            result_df.dropna(subset=['Tradedays_str', 'Year'], inplace=True)
            result_df['forindex'] = pandas.to_datetime(result_df['Tradedays_str'])
            result_df.drop_duplicates(subset=['forindex'], inplace=True)
            result_df.set_index('forindex', inplace=True)
            result_df.sort_index(inplace=True)

            result_df.loc[:,'actiondate'] = result_df.index.date # result_df['Tradedays_str']
            result_df['Tradedays_str'].where(result_df['isTradingday'],inplace=True)
            result_df['Year'].where(result_df['isTradingday'],inplace=True)
            result_df['day'].where(result_df['isTradingday'],inplace=True)
            result_df['Month'].where(result_df['isTradingday'],inplace=True)
            result_df['Tradedays'].where(result_df['isTradingday'],inplace=True)

            result_df.bfill(inplace=True)

            result_df['last_trading_date_2'] = result_df['Tradedays'].shift(2)
            result_df['last_trading_date'] = result_df['Tradedays'].shift(1)
            result_df['next_trading_date'] = result_df['Tradedays'].shift(-1)
            result_df['next_trading_date_2'] = result_df['Tradedays'].shift(-2)

            while not result_df.loc[result_df['Tradedays']== result_df['next_trading_date'],].empty:
                result_df['next_trading_date_2'].where(result_df['next_trading_date'] != result_df['Tradedays'], inplace=True)
                result_df['next_trading_date'].where(result_df['next_trading_date'] != result_df['Tradedays'], inplace=True)
                result_df.bfill(inplace=True)

            while not result_df.loc[result_df['next_trading_date']== result_df['next_trading_date_2'],].empty:
                result_df['next_trading_date_2'].where(result_df['next_trading_date_2'] != result_df['next_trading_date'], inplace=True)
                result_df.bfill(inplace=True)
            self._market_date_dict[exchangeid] = result_df
            result_df_tmp = result_df.copy()
            if start_date is not None:
                result_df_tmp = result_df.loc[pandas.to_datetime(start_date).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None):]
            if end_date is not None:
                result_df_tmp = result_df.loc[:pandas.to_datetime(end_date).replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=None)]
            return result_df_tmp
        return None

    @staticmethod
    def update_strategy_status(strategy_id,strategy_type,meta_data):
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('strategy_log')
        cl = db.get_collection(strategy_type)
        for k in meta_data.keys():
            if isinstance(meta_data[k], datetime.datetime):
                meta_data[k] =meta_data[k].strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(meta_data[k], pandas.Series):
                meta_data[k] = meta_data[k].to_dict()
            elif isinstance(meta_data[k], set):
                meta_data[k] = list(meta_data[k])
        meta_data_str = json.dumps(meta_data)

        setdata = {
                'meta_data':meta_data_str
                ,'update_time':datetime.datetime.now()
            }
        for col in ['sharpe_ratio','sortino_ratio','calmar_ratio','max_drawdown']:
            if col in meta_data:
                setdata[col] = meta_data[col]

        ret = cl.update_one(filter={'strategy_id':strategy_id}
                            ,update={'$set':setdata
            }
                            ,upsert=True
                            )
        if ret.upserted_id is not None:
            return ret.upserted_id
        return strategy_id

    @staticmethod
    def get_strategy_status(strategy_id=None,strategy_type=None):
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('strategy_log')
        cl = db.get_collection('strategy')
        query = {}
        if strategy_id is not None:
            query['strategy_id'] = strategy_id
        if strategy_type is not None:
            query['strategy_type'] = strategy_type
        ret = cl.find(query)
        ret_df = pandas.DataFrame(ret[:])
        ret_df.set_index('strategy_id',inplace=True)
        return ret_df