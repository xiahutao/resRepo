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
# jq.auth('15160067538','1234567890')
# jq.auth('13155110265','110265')
jq.auth('13606605554','Juzheng2008')

mongo_client = DataFactory.get_mongo_client()
arc = Arctic(mongo_client)
libstr = setting.INDEX_WEIGHT_LIB

if not arc.library_exists(libstr):
    arc.initialize_library(libstr,lib_type=CHUNK_STORE)
arclib = arc.get_library(libstr)

trading_date_his = DataFactory.get_trading_date()

index_info = DataFactory.get_index_info()
for idx , row in index_info.iterrows():
    symbol = idx[1]
    print(symbol)
    if arclib.has_symbol(symbol):
        continue
    start_date = row['start_date']
    end_date = row['end_date']
    trading_date_his_tmp = trading_date_his.loc[pandas.to_datetime(start_date):pandas.to_datetime(end_date)]
    trading_date_his_tmp = trading_date_his_tmp.loc[:datetime.datetime.now()]

    trading_date_his_tmp.sort_index(ascending=False,inplace=True)
    last_weight_date = None
    index_weights_date2symbol_list = []
    for dt,row_dt in trading_date_his_tmp.iterrows():
        isTradingday = row_dt['isTradingday']
        if not isTradingday:
            continue
        if last_weight_date is not None and dt > last_weight_date:
            continue
        index_weights = jq.get_index_weights(index_id=symbol, date=dt)
        if index_weights.empty:
            continue
        date = index_weights['date'].max()
        last_weight_date = pandas.to_datetime(date)
        index_weights.index.name= 'symbol'
        index_weights.reset_index(inplace=True)
        index_weights_date2symbol = index_weights.pivot(index='date',columns='symbol',values='weight')
        index_weights_date2symbol_list.append(index_weights_date2symbol)

    index_weights = pandas.concat(index_weights_date2symbol_list)
    metadata = {'t': 'dataframe', 'sy': symbol, 's': index_weights.iloc[0].name, 'e': index_weights.iloc[-1].name,
                'l': len(index_weights), 'update': datetime.datetime.now()}

    arclib.delete(symbol=symbol)
    arclib.append(symbol=symbol, item=index_weights, metadata=metadata, chunk_size='Y', upsert=True)
#
#
# securities = jq.get_index_weights(index_id=ind, date=d)
#
# mongo_client = DataFactory.get_mongo_client()
# db = mongo_client.get_database('MARKET')
# collection = db.get_collection('instruments_jq')
# for idx, row in securities.iterrows():
#     row_dict = row.to_dict()
#     row_dict['symbol'] = idx
#     row_dict['ASSET_TYPE']= setting.ASSETTYPE_INDEX
#     collection.update_one(filter={'symbol':row_dict['symbol']},update={'$set':row_dict},upsert=True)

