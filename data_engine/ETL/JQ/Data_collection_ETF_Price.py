#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/11/14 8:38
# @Author  : jwliu
# @Site    : 
# @File    :
# @Software: PyCharm

import pandas
import numpy
import datetime
from data_engine.data_factory import DataFactory
import data_engine.setting as setting
import jqdatasdk as jq
from arctic import Arctic,exceptions,CHUNK_STORE
from arctic.date import string_to_daterange

import arctic

jq.auth('15160067538','1234567890')
mongo_client = DataFactory.get_mongo_client()

arc = Arctic(mongo_client)
if not arc.library_exists(setting.STOCK_DAILY_JQ_LIB):
    arc.initialize_library(setting.STOCK_DAILY_JQ_LIB,lib_type=CHUNK_STORE)
arclib = arc.get_library(setting.STOCK_DAILY_JQ_LIB)

stocks_jq = DataFactory._get_instruments(asset_type=setting.ASSETTYPE_ETF,collection='instruments_jq')
stocks_jq.reset_index(inplace=True)

for idx, row in stocks_jq.iterrows():
    symbol = row['symbol']
    start_date = row['start_date']
    end_date = row['end_date']
    if pandas.to_datetime(start_date) > datetime.datetime.now():
        continue
    etfPrices_df = jq.get_price(symbol, start_date=start_date, end_date=end_date,
                                fields=['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']
                                ,frequency='daily', skip_paused=False, fq='pre')
    etfPrices_df.dropna(how='all',inplace=True)
    print(symbol)
    if etfPrices_df.empty:
        print(symbol,'empty')
        continue
    etfPrices_df.index.name = 'date'
    etfPrices_df = etfPrices_df[:datetime.datetime.now()]
    metadata = {'t':'dataframe','sy':symbol,'s':etfPrices_df.iloc[0].name,'e':etfPrices_df.iloc[-1].name,'l':len(etfPrices_df),'update':datetime.datetime.now()}

    arclib.delete(symbol=symbol)
    arclib.append(symbol=symbol,item=etfPrices_df,metadata=metadata,chunk_size='Y',upsert=True)

# stocks.reset_index(inplace=True)
# stocks_jq.reset_index(inplace=True)
# stocks_jq = pandas.merge(stocks_jq,stocks[['symbol','sec_name']].rename(columns={'symbol':'symbol_wind'}),how='left',right_on='sec_name'
#                          ,left_on='display_name',suffixes=('','_wind'))
# for idx, row in stocks_jq.iterrows():
#     row_dict = {}
#     row_dict['symbol_wind'] = row['symbol_wind']
#     row_dict['symbol'] = row['symbol']
#     collection.update_one(filter={'symbol':row_dict['symbol']},update={'$set':{
#         'symbol_wind':row['symbol_wind'],
#         'ASSET_TYPE': setting.ASSETTYPE_ETF
#     }})
