#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/3/9 10:00
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import os
import datetime
import copy
import pandas
import pytz
import pymongo
import data_engine.global_variable as global_variable
from data_engine.data_factory import DataFactory
from common.mongo_object import mongo_object
from common.os_func import check_fold
from portfolio_management.account import Account
from config.config import Config_trading
from analysis.analysis import Analysis_func
from analysis.sector_analysis import SectorAnalysis
from common.dict_helper import dict_helper

from reportlab.platypus import Table, SimpleDocTemplate, Paragraph,NextPageTemplate,PageBreak,PageBegin
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph,Spacer,Image,Table
from reportlab.lib.units import cm
from analysis.report.graphs import Graphs

import numpy

class Strategy(mongo_object):
    '''
    策略权重和品种scaler控制
    '''
    WEIGHT_TYPY_OPT = 'opt'
    WEIGHT_TYPY_AVG = 'avg'

    NAME_SUFFIX_BY_OPT = '_by_opt'
    def __init__(self,strategy_name,account=None, name_suffix=None):
        mongo_object.__init__(self)
        self._strategy_name = strategy_name
        self._strategy = None
        self._strategy_aggtoken_his = None
        self._strategy_vol = None
        self._name_suffix= name_suffix
        self._strategy_daily_returns = None
        if account is None:
            account = Account()
        self._account = account
        self.load()
    def summary(self,date=None):
        if date is None:
            date = global_variable.get_now()
        strategy_summary = pandas.Series()
        strategy_summary['name'] = self._strategy_name
        strategy_summary['vol'] = self.strategy_vol(date=date)
        strategy_summary['target_vol'] = self.strategy_target_vol()
        strategy_summary['scaler'] = self.strategy_scaler(date=date)
        strategy_summary['capital'] = self.capital(date=date)

        agg_summary = pandas.DataFrame(columns=['agg','target_vol','weight','capital'])
        for agg in self.list_aggtoken():
            agg_summary.loc[len(agg_summary)] = pandas.Series({
                'agg':agg, 'target_vol':self.aggtoken_target_vol(aggtoken=agg), 'weight':self.aggtoken_weight(aggtoken=agg), 'capital':self.aggtoken_capital(aggtoken=agg,date=date)
            })
        print(pandas.DataFrame(strategy_summary).T)
        print(agg_summary)
        return strategy_summary,agg_summary

    def summary_report(self,pdf_filename=None,date=None, pdf_path=None):
        if date is None:
            date = global_variable.get_now()
        if pdf_filename is None:
            pdf_filename = 'summary_report_' + self._strategy_name + '(%s)' % date.strftime('%Y%m%d') + '.pdf'
        strategy_summary, agg_summary = self.summary(date)
        strategy_summary = pandas.DataFrame(strategy_summary).T
        strategy_summary = numpy.round(strategy_summary,4)
        agg_summary = numpy.round(agg_summary,4)

        styles = getSampleStyleSheet()
        content = list()
        content.append(
            Graphs.draw_title('summary_report_' + self._strategy_name + '(%s)' % date.strftime('%Y%m%d')))
        content.append(Spacer(0, 0.5 * cm))

        data = [tuple(strategy_summary.columns)] + [tuple(x.to_dict().values()) for idx, x in strategy_summary.iterrows()]
        content.append(Paragraph('ALL: ', styles['Heading1']))
        content.append(Graphs.draw_table(*data, ALIGN='LEFT', VALIGN='RIGHT',
                                         col_width=[80] + [150] * (len(strategy_summary.columns) - 1)))
        content.append(Spacer(0, 0.5 * cm))

        data = [tuple(agg_summary.columns)] + [tuple(x.to_dict().values()) for idx, x in agg_summary.iterrows()]
        content.append(Paragraph('ALL: ', styles['Heading1']))
        content.append(Graphs.draw_table(*data, ALIGN='LEFT', VALIGN='RIGHT',
                                         col_width=[80] + [150] * (len(agg_summary.columns) - 1)))
        content.append(Spacer(0, 0.5 * cm))

        filename = pdf_filename
        if pdf_path is not None:
            check_fold(pdf_path)
            filename = os.path.join(pdf_path, pdf_filename)
        doc = SimpleDocTemplate(filename, pagesize=letter)
        doc.build(content)

    @property
    def is_disabled(self):
        if self._strategy is None:
            return None
        if 'disabled' not in self._strategy:
            return None
        return self._strategy['disabled']

    @property
    def weight_type(self):
        if self._strategy is None:
            return None
        if 'weight_type' not in self._strategy:
            return None
        return self._strategy['weight_type']

    def with_account(self,account_obj):
        assert isinstance(account_obj,Account)
        self._account = account_obj
        return self

    def load(self):
        '''
        加载策略基础信息
        :return:
        '''
        if self._strategy_name is None:
            return self
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy')
        self._strategy = cli.find_one({'strategy': self._strategy_name},{'_id':0})
        return self

    def get_last_opt_weight_mean(self,aggtoken):
        retlist = self.get_last_opt_weight(aggtoken=aggtoken)
        retlist = [pandas.Series(x) for x in retlist]
        retdf = pandas.DataFrame(retlist)
        if retdf.empty:
            return 0
        return retdf['weight'].mean()

    def get_opt_weight_df(self,aggtoken):
        mongo_object = self.mongo_client
        db = mongo_object.get_database('portfolio')
        cl = db.get_collection('strategy_opt_weight')
        result = cl.find({'strategy':self._strategy_name,'aggtoken':aggtoken},{'_id':0})
        list_result = list(result[:])
        if len(list_result) == 0:
            return None
        df = pandas.DataFrame(list_result)
        sub_df_tmp_list = []
        for each,sub_df in df.groupby(['strategy','aggtoken','opt_method','lookback','freq']):
            sub_df_tmp = sub_df.set_index('date')['weight'].sort_index()
            sub_df_tmp.name = '_'.join(list(each))
            sub_df_tmp_list.append(sub_df_tmp)
        weight_panel = pandas.concat(sub_df_tmp_list,axis=1).sort_index()
        weight_panel = weight_panel.dropna(how='all').ffill()
        weight_panel['mean'] = weight_panel.mean(axis=1)
        return weight_panel

    def get_last_opt_weight(self,aggtoken):
        mongo_object = self.mongo_client
        db = mongo_object.get_database('portfolio')
        cl = db.get_collection('strategy_opt_weight')
        result = cl.aggregate(pipeline=[
                { '$match': {'strategy':self._strategy_name,'aggtoken':aggtoken}},
                {'$sort': {'date': 1}},
                {'$group':{'_id':{'opt_method':'$opt_method','lookback':'$lookback','freq':'$freq'}
                    ,'weight':{'$last':'$weight'} ,'date':{'$last':'$date'}
                           }}
            ])
        ret_list = []
        for i in result:
            ret = i['_id']
            for k,v in i.items():
                if k == '_id':
                    continue
                ret[k] = v
            ret['strategy'] = self._strategy_name
            ret['aggtoken'] = aggtoken
            ret_list.append(ret)
        return ret_list

    def list_aggtoken_opt_method(self,aggtoken):
        mongo_object = self.mongo_client
        assert isinstance(mongo_object,pymongo.MongoClient)
        db = mongo_object.get_database('portfolio')
        cl = db.get_collection('strategy_opt_weight')
        return cl.distinct('opt_method',filter={'strategy':self._strategy_name,'aggtoken':aggtoken})


    def load_strategy_aggtoken_his(self,aggtoken=None):
        '''
        品种波动率历史
        :param aggtoken:
        :return:
        '''
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_aggtoken_his')
        if aggtoken is None:
            ret = cli.find({'strategy': self._strategy_name},{'_id':0})
        else:
            ret = cli.find({'strategy': self._strategy_name,'aggtoken':aggtoken}, {'_id': 0})
        ret_list = list(ret[:])
        if len(ret_list)>0:
            if self._strategy_aggtoken_his is None:
                self._strategy_aggtoken_his = {}
            df = pandas.DataFrame(ret_list).set_index('date')
            df.index = (pandas.to_datetime(pandas.DatetimeIndex(df.index).date) + datetime.timedelta(hours=15)).tz_localize(global_variable.DEFAULT_TIMEZONE)
            if aggtoken is None:
                strategy_aggtoken_his = {x:y for x,y in df.groupby('aggtoken')}
                for x,y in strategy_aggtoken_his.items():
                    y = y.sort_index()
                    y['volatility_shift2'] = y['volatility'].shift(2)
                    self._strategy_aggtoken_his[x] = y
            else:
                y = df
                y = y.sort_index()
                y['volatility_shift2'] = y['volatility'].shift(2)
                self._strategy_aggtoken_his[aggtoken] = y
            return self

    def load_strategy_vol(self):
        '''
        策略波动率历史
        :return:
        '''
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_his')
        volatility = 'volatility'
        if self._name_suffix is not None:
            volatility = 'volatility' + self._name_suffix

        result = cli.find({'strategy': self._strategy_name}, {'_id': 0, 'date': 1, volatility: 1})

        df = pandas.DataFrame(list(result[:])).set_index('date')
        if self._name_suffix is not None:
            df = df.rename(columns={volatility:'volatility'})
        df.index = (pandas.to_datetime(pandas.DatetimeIndex(df.index).date) + datetime.timedelta(hours=15)).tz_localize(global_variable.DEFAULT_TIMEZONE)
        df['volatility_shift2'] = df['volatility'].shift(2)
        self._strategy_vol = df
        return self

    def calc_strategy_daily_returns(self, by_opt_weight=True):
        '''
        合计品种得到策略日收益
        :return:
        '''
        if self._strategy_aggtoken_his is None:
            return None

        subdf_list = []
        for aggtoken,subdf in self._strategy_aggtoken_his.items():
            # target_vol = self.aggtoken_target_vol(aggtoken=aggtoken)
            # subdf['volatility_shift2'] = subdf['volatility'].shift(2)
            # subdf.loc[subdf['volatility_shift2'] < 0.01, 'volatility_shift2'] = 0.01
            # subdf['aggtoken_scaler'] = target_vol / subdf['volatility_shift2']
            # subdf['daily_return_byscaler'] = subdf['daily_return'] * subdf['aggtoken_scaler']
            # daily_return_byscaler = subdf['daily_return_byscaler']
            # daily_return_byscaler.name = aggtoken
            daily_return = subdf['daily_return']
            daily_return.name = aggtoken

            if by_opt_weight:
                wgt_df = self.get_opt_weight_df(aggtoken=aggtoken)
                wgt_df.index = (pandas.to_datetime(wgt_df.index) + datetime.timedelta(hours=15)).tz_localize(global_variable.DEFAULT_TIMEZONE)
                newdf = pandas.concat([daily_return,wgt_df['mean'].shift(2)],axis=1)
                newdf.columns = ['return','weight']
                newdf['return'] = newdf['return'].fillna(0)
                newdf['weight'] = newdf['weight'].ffill()
                daily_return = newdf['return'] * newdf['weight']
                daily_return.name = aggtoken
            subdf_list.append(daily_return)
        sa = SectorAnalysis(sector=self._strategy_name, daily_returns_list=subdf_list,by_mean= not by_opt_weight)
        self._strategy_daily_returns = sa._daily_returns
        return self._strategy_daily_returns


    def enddate_of_aggtoken_opt_weight(self,aggtoken):
        last_opt_weight = self.get_last_opt_weight(aggtoken=aggtoken)
        if len(last_opt_weight) == 0:
            return None
        ret = None
        for each in last_opt_weight:
            date = pandas.to_datetime(each['date'])
            if ret is None or ret > date:
                ret = date
        return ret

    @property
    def enddate_of_strategy_daily_returns(self):
        if self._strategy_daily_returns is None:
            return None
        return self._strategy_daily_returns.index[-1]

    def enddate_of_aggtoken_daily_returns(self,aggtoken):
        if self._strategy_aggtoken_his is None or aggtoken not in self._strategy_aggtoken_his:
            return None
        return self._strategy_aggtoken_his[aggtoken].index[-1]

    @staticmethod
    def list_strategy():
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy')
        result = cli.find({},{'_id':0,'strategy':1})
        result_list = list(result[:])
        if result_list is None:
            return None
        return [x['strategy'] for x in result_list]

    def list_aggtoken(self):
        obj = dict_helper.get_dict(self._strategy,'aggtokens')
        if obj is None:
            return None
        return list(obj.keys())

    # def list_aggtoken_opt_method(self,aggtoken):
    #     obj = dict_helper.get_dict(self._strategy,'aggtokens',aggtoken,'opt_weights')
    #     if obj is None:
    #         return None
    #     return list(obj.keys())

    def is_exist(self,aggtoken):
        if self._strategy is None:
            self.load()
        if aggtoken in self.list_aggtoken():
            return True
        return False

    def sum_aggtoken_weight(self):
        obj = dict_helper.get_dict(self._strategy,'aggtokens')
        if obj is None:
            return None
        return sum(list(obj.values()))

    @staticmethod
    def register_strategy(strategy,aggtokens=None,disabled=True):
        '''
        注册策略
        :param strategy:
        :param aggtokens:
        :param disabled:
        :return:
        '''
        if strategy in Strategy.list_strategy():
            return
        if aggtokens is None:
            aggtokens = {}
        mongo_client = DataFactory.get_mongo_client()
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy')
        cli.insert({'strategy':strategy,'aggtokens':aggtokens,'disabled':disabled,'weight_type':'opt'})

    def register_aggtoken(self,aggtoken,target_vol,weight=None):
        '''
        注册标的品种
        :param aggtoken:
        :param target_vol:
        :param weight:
        :return:
        '''
        result = self._strategy
        if 'aggtokens' not in result:
            result['aggtokens'] = {}
        result['aggtokens'][aggtoken] = {'target_vol':target_vol,'weight':weight}
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy')
        cli.update_one({'strategy':self._strategy_name},{'$set':{'aggtokens':result['aggtokens']}})
        self.load()



    def last_weight_date(self,opt_method,freq,aggtoken=None,look_back=None):
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_opt_weight')
        filter = {'strategy': self._strategy_name, 'opt_method': opt_method, 'freq': freq}
        if aggtoken is not None:
            filter['aggtoken'] = aggtoken
        if look_back is not None:
            filter['lookback'] = look_back
        ret = cli.find_one(filter=filter
                           ,projection={'_id':0,'date':1}, sort=[('date', -1)])
        if ret is not None:
            return ret['date']
        return None

    def update_weight_by_opt_method(self,fromdate_weight_dict
                                    ,aggtoken,opt_method,look_back,freq
                                    , replace_all=False):
        if isinstance(fromdate_weight_dict,dict) and len(fromdate_weight_dict)>0:
            mongo_client = self.mongo_client
            db = mongo_client.get_database('portfolio')
            cli = db.get_collection('strategy_opt_weight')
            newdict2_list = []
            for fromdate,weight in fromdate_weight_dict.items():
                newdict = {'strategy': self._strategy_name, 'aggtoken': aggtoken, 'opt_method': opt_method, 'lookback': look_back,
                           'freq': freq,'date':fromdate,'weight':weight}
                if not replace_all:
                    cli.update_one({'strategy':self._strategy_name,'aggtoken':aggtoken,'opt_method':opt_method,'lookback':look_back,'freq':freq,'date':fromdate}
                                   ,{'$set':{'weight':weight}},upsert=True)
                else:
                    newdict2_list.append(newdict)
            if replace_all:
                cli.delete_many(filter={'strategy':self._strategy_name,'aggtoken':aggtoken,'opt_method':opt_method,'lookback':look_back,'freq':freq})
                cli.insert_many(newdict2_list)

    def update_weight(self,by='average'):
        '''
        按平均方式更新各品种权重
        :param by:
        :return:
        '''
        self.load()
        result = self._strategy
        if by == 'average':
            list_aggtoken = self.list_aggtoken()
            w = 1.0 / len(list_aggtoken)
            mongo_client = self.mongo_client
            db = mongo_client.get_database('portfolio')
            cli = db.get_collection('strategy')
            for aggtoken in list_aggtoken:
                if 'aggtokens' not in result:
                    result['aggtokens'] = {}
                result['aggtokens'][aggtoken]['weight'] = w
                cli.update_one({'strategy': self._strategy_name}, {'$set': {'aggtokens': result['aggtokens']}})
        self.load()

    def aggtoken_weight(self,aggtoken):
        obj = 0
        if self.weight_type == self.WEIGHT_TYPY_AVG:
            obj = dict_helper.get_dict(self._strategy,'aggtokens',aggtoken,'weight')
        elif self.weight_type == self.WEIGHT_TYPY_OPT:
            obj = self.get_last_opt_weight_mean(aggtoken=aggtoken)
        if obj is None or numpy.isnan(obj):
            return 0
        return obj

    def aggtoken_target_vol(self,aggtoken):
        obj = dict_helper.get_dict(self._strategy,'aggtokens',aggtoken,'target_vol')
        if obj is None:
            return 0
        return obj

    def strategy_target_vol(self):
        return self._account.strategy_target_vol(strategy = self._strategy_name)

    def strategy_vol(self,date=None):
        if self._strategy_vol is None or self._strategy_vol.empty:
            self.load_strategy_vol()
        if self._strategy_vol is None or self._strategy_vol.empty:
            raise Exception('load strategy_vol firstly')
        if date is None:
            return self._strategy_vol['volatility_shift2']


        if date > self._strategy_vol.iloc[-1].name and len(self._strategy_vol) > 1:
            return self._strategy_vol.iloc[-2]['volatility_shift2']
        df = self._strategy_vol[date:]
        if df.empty:
            return self._strategy_vol.iloc[-1]['volatility_shift2']
        return df.iloc[0]['volatility_shift2']

    def strategy_scaler(self,date=None):
        target_vol = self.strategy_target_vol()
        vol = self.strategy_vol(date=date)
        if isinstance(vol,pandas.Series):
            vol[ vol < 0.01] = 0.01
        elif vol < 0.01:
            vol = 0.01
        scaler = target_vol / vol
        if isinstance(scaler,pandas.Series):
            scaler.name = 'strategy_scaler'
        return scaler

    def strategy_aggtoken_scaler(self,aggtoken,date=None):
        target_vol = self.aggtoken_target_vol(aggtoken=aggtoken)
        vol = self.strategy_aggtoken_vol(aggtoken=aggtoken,date=date)
        if isinstance(vol,pandas.Series):
            vol[ vol < 0.01] = 0.01
        elif vol < 0.01:
            vol = 0.01
        scaler = target_vol / vol
        if isinstance(scaler,pandas.Series):
            scaler.name = 'strategy_aggtoken_scaler'
            scaler = scaler.replace(-numpy.inf,1).replace(numpy.inf,1).replace(numpy.nan,1)
        return scaler

    def strategy_aggtoken_vol(self,aggtoken,date=None):
        if self._strategy_aggtoken_his is None:
            self.load_strategy_aggtoken_his()
        if self._strategy_aggtoken_his is None:
            # raise Exception('load strategy_aggtoken_his firstly')
            return None
        if aggtoken not in self._strategy_aggtoken_his:
            raise Exception('no aggtoken vol data of %s' % aggtoken)
            return None
        if date is None:
            return self._strategy_aggtoken_his[aggtoken]['volatility_shift2']
        if date > self._strategy_aggtoken_his[aggtoken].iloc[-1].name and len(self._strategy_aggtoken_his[aggtoken]) > 1:
            return self._strategy_aggtoken_his[aggtoken].iloc[-2]['volatility_shift2']
        df = self._strategy_aggtoken_his[aggtoken][date:]
        if df.empty:
            return self._strategy_aggtoken_his[aggtoken].iloc[-1]['volatility_shift2']
        return df.iloc[0]['volatility_shift2']

    def capital(self,date=None):
        '''
        策略总资金
        :param date:
        :return:
        '''
        if self._account is None:
            return None
        strategy_capital = self._account.strategy_capital(strategy=self._strategy_name)
        strategy_scaler = self.strategy_scaler(date=date)
        return strategy_capital * strategy_scaler

    def aggtoken_capital(self,aggtoken,date=None):
        '''
        品种资金
        :param aggtoken:
        :param date:
        :return:
        '''
        if self._account is None:
            return None
        capital = self.capital(date=date)
        aggtoken_weight = self.aggtoken_weight(aggtoken=aggtoken)
        return capital * aggtoken_weight

    def upload_dailyreturn(self,aggtoken,daily_return,fromdate=None,todate=None):
        '''
        更新入库品种日收益
        :param aggtoken:
        :param daily_return:
        :param fromdate:
        :param todate:
        :return:
        '''
        daily_return_tmp = Analysis_func.cut_returns(daily_returns=daily_return,look_back_start=fromdate,look_back_end=todate)
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_aggtoken_his')
        for x ,v in daily_return_tmp.items():
            cli.update_one(filter={'strategy':self._strategy_name,'aggtoken':aggtoken,'date': pandas.to_datetime(x)},
                           update={'$set':{'daily_return':v,'updatetime':datetime.datetime.now().astimezone(global_variable.get_timezone())}},
                           upsert=True)

    def upload_volatility(self,aggtoken,daily_return,rolling_window = 252,fromdate=None,todate=None):
        '''

        更新入库品种波动率
        :param aggtoken:
        :param daily_return:
        :param rolling_window:
        :param fromdate:
        :param todate:
        :return:
        '''
        # volatility = daily_return.rolling(window=rolling_window).std() * (252 ** 0.5)
        volatility = Analysis_func.volatility(daily_returns=daily_return,span=rolling_window) # daily_return.ewm(span=rolling_window,min_periods=rolling_window,adjust=False).std(bias=True)* (252 ** 0.5)
        daily_return_tmp = Analysis_func.cut_returns(daily_returns=volatility,look_back_start=fromdate,look_back_end=todate)
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_aggtoken_his')
        for x ,v in daily_return_tmp.items():
            cli.update_one(filter={'strategy':self._strategy_name,'aggtoken':aggtoken,'date': pandas.to_datetime(x)},
                           update={'$set':{'volatility':v,'v_updatetime':datetime.datetime.now().astimezone(global_variable.get_timezone())}},
                           upsert=True)

    def upload_position(self,aggtoken,position,fromdate=None,todate=None):
        '''
        '''
        position_tmp = Analysis_func.cut_returns(daily_returns=position,look_back_start=fromdate,look_back_end=todate)
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_aggtoken_his')
        for x ,v in position_tmp.items():
            cli.update_one(filter={'strategy':self._strategy_name,'aggtoken':aggtoken,'date': pandas.to_datetime(x)},
                           update={'$set':{'position':v,'p_updatetime':datetime.datetime.now().astimezone(global_variable.get_timezone())}},
                           upsert=True)


    def upload_strategy_dailyreturn(self,daily_return,fromdate=None,todate=None, name_suffix=None):
        '''

        更新入库策略日收益
        :param daily_return:
        :param fromdate:
        :param todate:
        :return:
        '''
        daily_return_tmp = Analysis_func.cut_returns(daily_returns=daily_return,look_back_start=fromdate,look_back_end=todate)
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_his')
        for x ,v in daily_return_tmp.items():
            if name_suffix is None:
                update = {'daily_return':v,'updatetime':datetime.datetime.now().astimezone(global_variable.get_timezone())}
            else:
                update = {'daily_return' + name_suffix:v,'updatetime' + name_suffix:datetime.datetime.now().astimezone(global_variable.get_timezone())}
            cli.update_one(filter={'strategy':self._strategy_name,'date': pandas.to_datetime(x)},
                           update={'$set':update},
                           upsert=True)
            
    def upload_strategy_volatility(self,daily_return,rolling_window = 252,fromdate=None,todate=None, name_suffix=None):
        '''

        更新入库策略波动率
        :param daily_return:
        :param rolling_window:
        :param fromdate:
        :param todate:
        :return:
        '''
        volatility = Analysis_func.volatility(daily_returns=daily_return,span=rolling_window) # daily_return.ewm(span=rolling_window,min_periods=rolling_window,adjust=False).std(bias=True)* (252 ** 0.5)
        daily_return_tmp = Analysis_func.cut_returns(daily_returns=volatility,look_back_start=fromdate,look_back_end=todate)
        mongo_client = self.mongo_client
        db = mongo_client.get_database('portfolio')
        cli = db.get_collection('strategy_his')
        for x ,v in daily_return_tmp.items():
            if name_suffix is None:
                update = {'volatility' :v,'v_updatetime':datetime.datetime.now().astimezone(global_variable.get_timezone())}
            else:
                update = {'volatility' + name_suffix :v,'v_updatetime'+ name_suffix:datetime.datetime.now().astimezone(global_variable.get_timezone())}
            cli.update_one(filter={'strategy':self._strategy_name,'date': pandas.to_datetime(x)},
                           update={'$set':update},
                           upsert=True)

    @staticmethod
    def gen_stop_loss_config(market,
                             instrument,
                             tradeStartTime,
                             tradeStopTime,
                             priceBarFreqSec,
                             priceBarOffset,
                             strategy,
                             aggToken,
                             stopPrice,
                             stopLowerBound,
                             stopTarget,
                           dump=False,
                           dump_path=None,
                           dump_file=None,
                           dump_append=False,
                           **kwargs):
        config = Config_trading()
        config.set_config_item('market', market)
        config.set_config_item('instrument', instrument)
        config.set_config_item('tradeStartTime', tradeStartTime)
        config.set_config_item('tradeStopTime', tradeStopTime)
        config.set_config_item('priceBarFreqSec', priceBarFreqSec)
        config.set_config_item('priceBarOffset', priceBarOffset)
        config.set_config_item('strategy', strategy)
        config.set_config_item('aggToken', aggToken)
        config.set_config_item('stopPrice', stopPrice)
        config.set_config_item('stopLowerBound', stopLowerBound)
        config.set_config_item('stopTarget', stopTarget)
        for x,v in kwargs.items():
            config.set_config_item(x,v)

        if dump:
            if dump_path is not None and dump_file is not None:
                check_fold(dump_path)
                config.dump_csv(config_file=os.path.join(dump_path,dump_file),append=dump_append)
        return config

    @staticmethod
    def gen_target_position_config(requestType,
                                   instrument,
                                   market,
                                   aggToken,
                                   requestTime,
                                   aggregateRequest,
                                   targetPosition,
                                   strategy,
                                   histLastSignalTime,
                                   initiator,

                                   dump=False,
                                   dump_path=None,
                                   dump_file=None,
                                   dump_append=False,
                                   **kwargs):
        config = Config_trading()
        config.set_config_item('requestType', requestType)
        config.set_config_item('instrument', instrument)
        config.set_config_item('market', market)
        config.set_config_item('aggToken', aggToken)
        config.set_config_item('requestTime', requestTime)
        config.set_config_item('aggregateRequest', aggregateRequest)
        config.set_config_item('targetPosition', targetPosition)
        config.set_config_item('strategy', strategy)
        config.set_config_item('histLastSignalTime', histLastSignalTime)
        config.set_config_item('initiator', initiator)
        for x,v in kwargs.items():
            config.set_config_item(x,v)

        if dump:
            if dump_path is not None and dump_file is not None:
                check_fold(dump_path)
                config.dump_csv(config_file=os.path.join(dump_path,dump_file),append=dump_append)
        return config

if __name__ == '__main__':

    DataFactory.config(MONGDB_PW='jz501241',MONGDB_USER='dbmanager_future',MONGDB_IP='192.168.2.201',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)
    import pymongo
    import copy
    cli = DataFactory.get_mongo_client()
    assert isinstance(cli,pymongo.MongoClient)
    db = cli.get_database('Trading_record')
    cl = db.get_collection('ORDER_REQUESTS')
    aggToken_list = cl.distinct('aggToken',filter={'strategy':'Pair-Intraday'})

    s = Strategy(strategy_name='Pair-Intraday')
    for agg in aggToken_list:
        if s.is_exist(aggtoken=agg):
            continue
        agg2 = agg.replace('_VOL','').split('_')
        agg2 = '_'.join([agg2[1] + '_VOL',agg2[0] + '_VOL'])
        print(agg,agg2,s.is_exist(agg2))

        cl.update_many(filter={'strategy':'Pair-Intraday','aggToken':agg},update={'$set':{'aggToken':agg2}})

    # # cl2.ensure_index({'strategy':1,'aggtoken':1,'opt_method':1,'lookback':1,'freq':1,'date':1})
    # for strategy in Strategy.list_strategy():
    # # strategy = 'Pair-Intraday'
    #     print(strategy)
    #     ret = cl.find_one({'strategy':strategy},{'_id':0})
    #     aggtokens = ret['aggtokens']
    #     for aggtoken, settings in aggtokens.items():
    #         if 'opt_weights' not in settings:
    #             continue
    #         opt_weights = settings['opt_weights']
    #         for opt_method, dict0 in opt_weights.items():
    #             newdict2_list = []
    #             for lookback, dict1 in dict0.items():
    #                 for freq, dict2 in dict1.items():
    #                     newdict = {'strategy':strategy,'aggtoken':aggtoken,'opt_method':opt_method,'lookback':lookback,'freq':freq}
    #                     for dt,value in dict2.items():
    #                         newdict2 = newdict
    #                         newdict2['date'] = dt
    #                         newdict2['weight'] = value
    #                         if numpy.isnan(value):
    #                             continue
    #                         newdict2_list.append(copy.copy(newdict2))
    #             cl2.delete_many(filter={'strategy':strategy,'aggtoken':aggtoken,'opt_method':opt_method})
    #             for each in newdict2_list:
    #                 cl2.insert(copy.copy(each))
    #         ret['aggtokens'][aggtoken].pop('opt_weights')
    #     cl.update_one(filter={'strategy':strategy},update= {'$set':{'aggtokens':ret['aggtokens']}},upsert=False)
    #     print(ret)
    # from portfolio_management.account import Account
    # from portfolio_management.strategy import Strategy
    from analysis.analysis import Analysis
    path = 'e:/tmp'
    check_fold(path)
    list_strategy = Strategy.list_strategy()
    for strategy_name in []:
        s = Strategy(strategy_name=strategy_name).load_strategy_aggtoken_his()
        if s.is_disabled:
            continue
        print(strategy_name)
        for agg in s.list_aggtoken():
            strategy_aggtoken_vol = s.strategy_aggtoken_vol(aggtoken=agg,date=global_variable.get_now())
            strategy_aggtoken_scaler = s.strategy_aggtoken_scaler(aggtoken=agg,date=global_variable.get_now())
            print(agg,strategy_aggtoken_vol,strategy_aggtoken_scaler,strategy_aggtoken_vol*strategy_aggtoken_scaler)
        # s.calc_strategy_daily_returns()
        # print(s.enddate_of_aggtoken_opt_weight(aggtoken='AU_VOL'))
        # s.summary_report()

        # daily_return_list = []
        # for agg,df in s._strategy_aggtoken_his.items():
        #     daily_return = df['daily_return']
        #     daily_return.name = agg
        #     sharpe_ratio = Analysis_func.sharpe_ratio(daily_returns=daily_return)
        #     # print(agg,sharpe_ratio)
        #     daily_return_list.append(daily_return)
        #
        #     analysisobj = Analysis(daily_returns=daily_return,result_folder=r'D:/resRepo/resRepo/portfolio_management/tmp/' + agg)
        #     analysisobj.plot_rolling_returns(show=False)
        #
        # sa = SectorAnalysis(sector=s._strategy_name, daily_returns_list=daily_return_list)
        # sharpe_ratio = Analysis_func.sharpe_ratio(daily_returns=sa._daily_returns)
        # print(s._strategy_name,sharpe_ratio)
        # analysisobj = Analysis(daily_returns=sa._daily_returns,result_folder=r'D:/resRepo/resRepo/portfolio_management/tmp/' + s._strategy_name)
        # analysisobj.plot_rolling_returns(show=False)
        # df = pandas.concat(daily_return_list,axis=1)
        # df.to_csv('daily_returns.csv')
    for nm in []:
        account = Strategy(strategy_name=nm,name_suffix=Strategy.NAME_SUFFIX_BY_OPT)#.with_account(account_obj=Account())
        if account.is_disabled:
            continue
        account.load_strategy_aggtoken_his()
        max_drawdown_dict = {}
        annual_volatility_dict = {}
        weight_list = []
        for agg,df in account._strategy_aggtoken_his.items():  # account._strategy_aggtoken_his.items()
            # if agg not in ['AL_VOL']:
            #     continue
            print(agg,df)
            daily_return  = df['daily_return']
            analysisobj = Analysis(daily_returns=daily_return,result_folder=os.path.join(path,nm,agg))
            # analysisobj.plot_all()
            # analysisobj.save_result()
            wgt_df = account.get_opt_weight_df(aggtoken=agg)
            wgt_df.index = (pandas.to_datetime(wgt_df.index) + datetime.timedelta(hours=15)).tz_localize(global_variable.DEFAULT_TIMEZONE)
            wgt_df.to_csv(os.path.join(path,nm,'wgt_df_%s.csv' % agg))
            newdf = pandas.concat([daily_return, wgt_df['mean'].shift(2)], axis=1)
            newdf.columns = ['return', 'weight']
            newdf['return'] = newdf['return'].fillna(0)
            newdf['weight'] = newdf['weight'].ffill()
            weight = newdf['weight']
            weight.name = agg
            weight_list.append(weight)

            max_drawdown = Analysis_func.max_drawdown(daily_returns=daily_return)
            annual_volatility = Analysis_func.annual_volatility(daily_returns=daily_return)
            max_drawdown_dict[agg] = max_drawdown
            annual_volatility_dict[agg] = annual_volatility

        weight_df = pandas.concat(weight_list,axis=1)
        weight_df.to_csv(os.path.join(path,nm,'weight_df.csv'))
        scaler = account.strategy_scaler()
        print(pandas.Series(max_drawdown_dict).sort_values())
        df = pandas.concat([pandas.Series(max_drawdown_dict),pandas.Series(annual_volatility_dict)],axis=1)
        df.columns = ['mdd','av']
        print(df.sort_values('mdd'))

        dr = account.calc_strategy_daily_returns()

        analysisobj = Analysis(daily_returns=dr,result_folder=os.path.join(path,nm,''))
        analysisobj.plot_all()
        analysisobj.save_result()

        substrategy_daily_returns_scalered = scaler * dr
        print(Analysis_func.max_drawdown(daily_returns=dr),Analysis_func.annual_volatility(daily_returns=dr))
        print(Analysis_func.max_drawdown(daily_returns=substrategy_daily_returns_scalered)
              ,Analysis_func.annual_volatility(daily_returns=substrategy_daily_returns_scalered))

        for i in range(2003,2020):
            print(Analysis_func.max_drawdown(daily_returns=dr,look_back_start=datetime.datetime(i,1,1).astimezone(global_variable.get_timezone()),look_back_end=datetime.datetime(i+1,1,1).astimezone(global_variable.get_timezone()))
                  ,Analysis_func.annual_volatility(daily_returns=dr,look_back_start=datetime.datetime(i,1,1).astimezone(global_variable.get_timezone()),look_back_end=datetime.datetime(i+1,1,1).astimezone(global_variable.get_timezone())))

        for i in range(2003,2020):
            print(Analysis_func.max_drawdown(daily_returns=substrategy_daily_returns_scalered,look_back_start=datetime.datetime(i,1,1).astimezone(global_variable.get_timezone()),look_back_end=datetime.datetime(i+1,1,1).astimezone(global_variable.get_timezone()))
                  ,Analysis_func.annual_volatility(daily_returns=substrategy_daily_returns_scalered,look_back_start=datetime.datetime(i,1,1).astimezone(global_variable.get_timezone()),look_back_end=datetime.datetime(i+1,1,1).astimezone(global_variable.get_timezone())))

        # print(account.weight_type)
        # w_sum = 0.0
        # for tkn in account.list_aggtoken():
        #     # print(nm,tkn, account.list_aggtoken_opt_method(aggtoken=tkn))
        #     df = account.get_opt_weight_df(aggtoken=tkn)
        #
        #     print(df)
        #     break
            # get_last_opt_weight_mean = account.get_last_opt_weight_mean(aggtoken=tkn)
            # if get_last_opt_weight_mean is not None:
            #     w_sum = w_sum + get_last_opt_weight_mean
            # print(get_last_opt_weight_mean, nm,tkn)
        # print(nm,w_sum)
    # print(account.aggtoken_capital(aggtoken='AG_VOL',date=global_variable.get_now()))
    # print(account.list_strategy())
    # print(account.list_aggtoken())
    # account.update_weight_by_opt_method(fromdate_weight_dict={'1':1,'2':2,'3':2}
    #                                     ,aggtoken='AG_VOL',opt_method='minimize_CVaR',look_back='63',freq=global_variable.FREQ_1W)
    # print(account.list_aggtoken_opt_method(aggtoken='AG_VOL'))
    # print(account.sum_aggtoken_weight())
    # for each in account.list_aggtoken():
    #     print(account.aggtoken_weight(aggtoken=each))
    #
    # print(account.aggtoken_capital(aggtoken='CU_VOL_NI_VOL'))