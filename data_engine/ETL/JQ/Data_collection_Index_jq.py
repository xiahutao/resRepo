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
# jq.auth('15160067538','1234567890')
jq.auth('13155110265','110265')

securities = jq.get_all_securities(types=['index'], date=None)

mongo_client = DataFactory.get_mongo_client()
db = mongo_client.get_database('MARKET')
collection = db.get_collection('instruments_jq')
for idx, row in securities.iterrows():
    row_dict = row.to_dict()
    row_dict['symbol'] = idx
    row_dict['ASSET_TYPE']= setting.ASSETTYPE_INDEX
    collection.update_one(filter={'symbol':row_dict['symbol']},update={'$set':row_dict},upsert=True)

