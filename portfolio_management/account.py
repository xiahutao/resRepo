#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/3/5 13:27
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import pymongo
import datetime
import copy
import pytz
import data_engine.global_variable as global_variable
from data_engine.data_factory import DataFactory
from common.mongo_object import mongo_object
from common.dict_helper import dict_helper

class Account(mongo_object):
    def __init__(self,account_name = 'simnow-1'):
        if account_name is None:
            account_name = 'simnow-1'
        mongo_object.__init__(self)
        self._account_name = account_name
        self._account = None
        self.load()

    @staticmethod
    def bak_account_capital_record(account_capital):
        account_capital = copy.copy(account_capital)
        mongo_client = DataFactory.get_mongo_client()
        assert isinstance(mongo_client,pymongo.MongoClient)
        db = mongo_client.get_database('portfolio')
        if account_capital is not None:
            cli_bak = db.get_collection('account_capital_bak')
            account_capital['insert_time'] = datetime.datetime.now().astimezone(tz=pytz.timezone(global_variable.DEFAULT_TIMEZONE))
            cli_bak.insert(account_capital)

    @staticmethod
    def update_strategy_weight(account_name, strategy, weight):
        mongo_client = DataFactory.get_mongo_client()
        assert isinstance(mongo_client,pymongo.MongoClient)
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('account')
        account_capital = cli.find_one({'account': account_name},{'_id':False})
        Account.bak_account_capital_record(account_capital=account_capital)

        account_capital = dict_helper.set_dict(account_capital,weight
                                               ,'strategy',strategy,'weight')
        cli.update_one({'account': account_name},update={'$set':account_capital},upsert=True)

    # @staticmethod
    # def update_aggtoken_weight(account_name, strategy,aggtoken, weight):
    #     mongo_client = DataFactory.get_mongo_client()
    #     assert isinstance(mongo_client,pymongo.MongoClient)
    #     db = mongo_client.get_database('portfolio')
    #     cli = db.get_collection('account')
    #     account_capital = cli.find_one({'account': account_name},{'_id':0})
    #     Account.bak_account_capital_record(account_capital=account_capital)
    #     if account_capital is None:
    #         account_capital = {}
    #         account_capital['account'] = account_name
    #     if 'strategy' not in account_capital:
    #         account_capital['strategy'] = {}
    #     if strategy not in account_capital['strategy']:
    #         account_capital['strategy'][strategy] = {}
    #     if 'aggtokens' not in account_capital['strategy'][strategy]:
    #         account_capital['strategy'][strategy]['aggtokens'] = {}
    #     account_capital['strategy'][strategy]['aggtokens'][aggtoken]=weight
    #     cli.update_one({'account': account_name},update={'$set':account_capital},upsert=True)

    @staticmethod
    def list_account():
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('account')
        result = cli.find({},{'_id':0,'account':1})
        result_list = list(result[:])
        if result_list is None:
            return None
        return [x['account'] for x in result_list]

    def load(self):
        if self._account_name is None:
            return self
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('account')
        self._account = cli.find_one({'account': self._account_name},{'_id':0})
        return self

    def list_strategy(self):
        result = self._account
        if result is None:
            return None
        return list(result['strategy'].keys())

    def strategy_target_vol(self,strategy):
        obj = dict_helper.get_dict(self._account,'strategy',strategy,'target_vol')
        if obj is None:
            return 0
        return obj

    def strategy_capital(self,strategy):
        obj = dict_helper.get_dict(self._account,'strategy',strategy,'weight')
        if obj is None:
            return 0
        return obj * self.capital()


    def capital(self):
        obj = dict_helper.get_dict(self._account,'capital')
        if obj is not None:
            return obj
        return 0

if __name__ == '__main__':
    from data_engine.data_factory import DataFactory
    from data_engine.setting import  DATASOURCE_REMOTE,DATASOURCE_LOCAL

    DataFactory.config(MONGDB_PW='password',MONGDB_USER='juzheng',DATASOURCE_DEFAULT=DATASOURCE_REMOTE)

    account = Account(account_name='simnow-1').load_account()
    print(account.list_account())
    print(account.list_strategy())
    # print(account.aggtoken_capital(strategy='Momentum-Daily',aggtoken='I_VOL'))

    # Account.update_strategy_weight(account_name='test',strategy='test-s',weight=0.5)
    # Account.update_aggtoken_weight(account_name='test',strategy='test-s',aggtoken='AL_VOL',weight=0.5)
    #
    # print(Account.list_account())
    # print('capital',account.capital())
    # print(' ')
    # list_strategy = account.list_strategy()
    # for each in list_strategy:
    #     print(each, account.strategy_capital(strategy=each),account.strategy_target_vol(strategy=each))
    # print(' ')
    # for each in list_strategy:
    #     list_aggtoken = account.list_aggtoken(strategy=each)
    #     print(' ')
    #     print(each,'sum_aggtoken_weight',account.sum_aggtoken_weight(strategy=each))
    #     for each_aggtoken in list_aggtoken:
    #         print(each_aggtoken,account.aggtoken_weight(strategy=each,aggtoken=each_aggtoken),account.aggtoken_capital(strategy=each,aggtoken=each_aggtoken))