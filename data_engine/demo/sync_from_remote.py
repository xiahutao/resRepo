# -*- coding: utf-8 -*-
import os
import sys
CurrentPath = os.path.dirname(__file__)
print(CurrentPath)
sys.path.append(CurrentPath.replace('data_engine/demo',''))
print(sys.path)

from data_engine.data_factory import DataFactory
from data_engine.setting import ASSETTYPE_FUTURE,ASSETTYPE_STOCK
from data_engine.setting import FREQ_TICK,FREQ_1D,FREQ_1M,FREQ_5M
from data_engine.setting import DATASOURCE_LOCAL
from quant_lib.instruments.future import Future
import data_engine.setting as setting

if __name__ == '__main__':

    # symbols = ['C']
    # freq_list = [FREQ_1M]
    # for freq in freq_list:
    #     for symbol in symbols:
    #         DataFactory.sync_from_remote(asset_type=ASSETTYPE_FUTURE, symbol=symbol, freq=freq)

    # DataFactory.sync_from_remote(asset_type=ASSETTYPE_FUTURE,symbol='CU',freq=FREQ_5M)
    #
    #
    # stock = Stock(symbol='000001.SZ',dataSource=DATASOURCE_LOCAL)
    # stock.get_market_data(freq_list=[FREQ_TICK],start_date='2010-01-01')
    # print(stock[FREQ_TICK])

    #从远端数据库加载数据
    symbols = ['RB','RB_VOL','A','A_VOL']
    freq_list = [FREQ_1M]
    price_type = setting.PRICE_TYPE_UN
    for freq in freq_list:
        DataFactory.sync_from_remote(asset_type=ASSETTYPE_FUTURE, symbol=symbols, freq=freq,price_type=price_type)
        stock_data = DataFactory().get_stock_market_data(freq=freq,symbols=symbols,start_date=None,end_date=None,price_type=price_type)
        assert stock_data[symbols[0],price_type] is not None

    # symbols = DataFactory.get_stock_symbols(collection='instruments')[0:10]
    # freq_list = [FREQ_1D]
    # price_type = setting.PRICE_TYPE_UN
    # for freq in freq_list:
    #     DataFactory.sync_from_remote(asset_type=ASSETTYPE_STOCK, symbol=symbols, freq=freq,price_type=price_type)
    #     stock_data = DataFactory().get_stock_market_data(freq=freq,symbols=symbols,start_date=None,end_date=None,price_type=price_type)
    #     assert stock_data[symbols[0],price_type] is not None

    # symbols = DataFactory.get_stock_symbols(collection='instruments_jq')[0:10]
    # freq_list = [FREQ_1D]
    # for price_type in [setting.PRICE_TYPE_UN,setting.PRICE_TYPE_POST,setting.PRICE_TYPE_PRE]:
    #     for freq in freq_list:
    #         DataFactory.sync_from_remote(asset_type=ASSETTYPE_STOCK, symbol=symbols, freq=freq,price_type=price_type)
    #         stock_data = DataFactory().get_stock_market_data(freq=freq,symbols=symbols,start_date=None,end_date=None,price_type=price_type)
    #         assert stock_data[symbols[0],price_type] is not None
    #         print(price_type,'done')
    #从本地文件加载数据
    # future = Future(symbol=symbol,dataSource=DATASOURCE_LOCAL)
    # future.get_market_data(freq_list=freq_list,start_date='2010-01-01')
    # for freq in freq_list:
    #     print(future[freq])