#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/11/14 8:38
# @Author  : jwliu
# @Site    : 
# @File    :
# @Software: PyCharm

import pandas
import numpy
from data_engine.data_factory import DataFactory
import data_engine.setting as setting
import jqdatasdk as jq
jq.auth('15160067538','1234567890')

etf = jq.get_all_securities(types=['etf'], date=None)

mongo_client = DataFactory.get_mongo_client()
db = mongo_client.get_database('MARKET')
collection = db.get_collection('instruments_jq')
for idx, row in etf.iterrows():
    row_dict = row.to_dict()
    row_dict['symbol'] = idx
    row_dict['ASSET_TYPE']= setting.ASSETTYPE_ETF
    collection.update_one(filter={'symbol':row_dict['symbol']},update={'$set':row_dict},upsert=True)


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# stocks = DataFactory._get_instruments(asset_type=setting.ASSETTYPE_ETF)
# stocks_jq = DataFactory._get_instruments(asset_type=setting.ASSETTYPE_ETF,collection='instruments_jq')
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
