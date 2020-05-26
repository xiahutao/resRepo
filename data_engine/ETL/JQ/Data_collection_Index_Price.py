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

# jq.auth('15160067538','1234567890')
jq.auth('13155110265','110265')
mongo_client = DataFactory.get_mongo_client()

arc = Arctic(mongo_client)

# self.preAdjPricesAll_df=jq.get_price(self.allInstruments_list, start_date='2005-01-01', end_date='2018-10-31', frequency='daily', skip_paused=False, fq='pre')
# self.unAdjPricesAll_df=jq.get_price(self.allInstruments_list, start_date='2005-01-01', end_date='2018-10-31', frequency='daily', skip_paused=False, fq=None)
# self.postAdjPricesAll_df = jq.get_price(self.allInstruments_list, start_date='2005-01-01', end_date='2018-12-31',frequency='daily', skip_paused=False, fq='post')

pricetype = {'un':setting.STOCK_DAILY_JQ_LIB}
for (fq,libstr) in pricetype.items():
    if fq == 'un':
        fq = None
    if not arc.library_exists(libstr):
        arc.initialize_library(libstr,lib_type=CHUNK_STORE)
    arclib = arc.get_library(libstr)
    stocks_jq = DataFactory._get_instruments(asset_type=setting.ASSETTYPE_INDEX,collection='instruments_jq')
    stocks_jq.reset_index(inplace=True)

    for idx, row in stocks_jq.iterrows():
        symbol = row['symbol']
        start_date = row['start_date']
        end_date = row['end_date']
        if pandas.to_datetime(start_date) > datetime.datetime.now():
            continue
        if arclib.has_symbol(symbol):
            continue
        print(symbol)
        etfPrices_df = jq.get_price(symbol, start_date=start_date, end_date=end_date,
                                    fields=['open', 'close', 'low', 'high', 'volume', 'money', 'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused']
                                    ,frequency='daily', skip_paused=False, fq=fq)
        etfPrices_df.dropna(how='all',inplace=True)
        if etfPrices_df.empty:
            print(symbol,'empty')
            continue
        etfPrices_df.index.name = 'date'
        etfPrices_df = etfPrices_df[:datetime.datetime.now()]
        metadata = {'t':'dataframe','sy':symbol,'s':etfPrices_df.iloc[0].name,'e':etfPrices_df.iloc[-1].name,'l':len(etfPrices_df),'update':datetime.datetime.now()}

        arclib.delete(symbol=symbol)
        arclib.append(symbol=symbol,item=etfPrices_df,metadata=metadata,chunk_size='Y',upsert=True)
