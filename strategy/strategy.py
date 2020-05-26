# -*- coding: utf-8 -*-
import pandas
import os
import numpy
import datetime
import socket
from common.cache import cache
from common.decorator import runing_time
from common.os_func import merge_lines
from common.mail import mail
from abc import abstractclassmethod
from config.config import Config_base
from data_engine.data_factory import DataFactory
import data_engine.global_variable as global_variable
from data_engine.instrument.future import Future
from data_engine.instrument.product import Product

from analysis.analysis import Analysis_ex
from execution.execution import Execution_ex
from settlement.settlement import Settlement_ex

from portfolio_management.account import Account
from portfolio_management.strategy import Strategy as pf_strategy

class strategy_helper:
    @staticmethod
    def merge_config_file(config_file_path,config_file_output_path,stoploss_file_output_path,next_business_date, current_business_date):
        # 合并temp目录的文件统一输出
        stoploss_lines = None
        config_night_lines = None
        config_day_lines = None
        temp_folder = os.path.join(config_file_path, 'temp')
        current_business_date = pandas.to_datetime(current_business_date)
        next_business_date = pandas.to_datetime(next_business_date)
        if stoploss_file_output_path is not None:
            stoploss_lines = merge_lines(folder=temp_folder,
                                filename_part_list=[next_business_date.strftime('%Y-%m-%d'), 'stoploss'],
                                target_file=os.path.join(stoploss_file_output_path
                                                         ,'_'.join(['stoploss_config',next_business_date.strftime('%Y%m%d')]) + '.csv')
                                )
        if config_file_output_path is not None:
            config_night_lines = merge_lines(folder=temp_folder,
                                filename_part_list=[current_business_date.strftime('%Y-%m-%d'), 'night'],
                                target_file=os.path.join(config_file_output_path
                                                         ,'_'.join(['cta_daily_requests',current_business_date.strftime('%Y%m%d'),'night']) + '.csv')
                                )
            config_day_lines = merge_lines(folder=temp_folder,
                                filename_part_list=[next_business_date.strftime('%Y-%m-%d'), 'day'],
                                target_file=os.path.join(config_file_output_path
                                                         ,'_'.join(['cta_daily_requests',next_business_date.strftime('%Y%m%d'),'day']) + '.csv')
                                )
        return config_day_lines,config_night_lines,stoploss_lines

class Strategy_func:
    @staticmethod
    @runing_time
    def _format_df(data1):
        data1['adjPrice'] = (1 + data1.price_return).cumprod()
        data1['cumDelta'] = data1['price_delta'].cumsum()
        data1['DATE_TIME'] = data1.index
        # data1['TIME'] = data1.index.strftime('%H:%M:%S') #.apply(lambda x: x.strftime('%H:%M:%S'))
        data1 = data1.between_time('09:00:00', '23:30:00')
        data1 = data1.between_time('16:30:00', '15:00:00')
        # data1 = data1[(data1.TIME <= '23:30:00') & (data1.TIME >= '09:00:00')]
        # data1 = data1[((data1.TIME >= '16:30:00') | (data1.TIME <= '15:00:00'))]
        data1['price_delta'] = data1.cumDelta.diff(1)
        data1['price_return'] = data1.adjPrice.pct_change(1)
        return data1

    @staticmethod
    def _tf_ewma(short_term, long_term, px):
        _, _, sign_diff_ewma = Strategy_func._tf_ewma_ex(short_term=short_term,long_term=long_term,px=px)
        return sign_diff_ewma

    @staticmethod
    def _tf_ewma_ex(short_term, long_term, px):
        st_ewma = px.ewm(span = short_term, min_periods = short_term).mean()
        lt_ewma = px.ewm(span = long_term, min_periods = short_term).mean()
        diff_ewma = st_ewma - lt_ewma
        return st_ewma,lt_ewma,numpy.sign(diff_ewma)

class Strategy(pf_strategy):
    _strategy_type = ''
    _strategy_name = ''
    minCount1D = 240
    def __init__(self,**kwargs):
        self._params = kwargs
        self._contract_size_dict = {}
        self._tick_size_dict = {}
        self._symbols = set()

        self._future_dict = {}
        self._market_data_dict = {}

        self._config = None
        self._strategy_name = None
        self._account_name = None
        self._aggToken = None # 投资标的（篮子）
        if 'strategy_name' in kwargs:
            self._strategy_name = kwargs['strategy_name']
        if 'account_name' in kwargs:
            self._account_name = kwargs['account_name']
        pf_strategy.__init__(self,strategy_name=self._strategy_name)
        self.load()
        self.with_account(account_obj=Account(self._account_name).load())



    @property
    def aggToken(self):
        return self._aggToken

    @property
    def underlying_product(self):
        return [self.aggToken]

    @aggToken.setter
    def aggToken(self,value):
        self._aggToken = value

    def update_daily_return(self):
        pass

    def get_capital(self):
        return 0

    @property
    def config(self):
        return self._config
    @config.setter
    def config(self,value):
        self._config = value

    @staticmethod
    def get_settle_obj(config,signal_dataframe):
        (success, positions_dataframe) = Execution_ex(config=config).exec_trading(signal_dataframe=signal_dataframe)
        if success:
            settlement_obj = Settlement_ex(config=config)
            settlement_obj.settle(positions_dataframe=positions_dataframe)
            return settlement_obj
        else:
            print(positions_dataframe)
        return None

    def format_data(self,data):
        return data

    def get_parma(self,key):
        if key in self._params:
            return self._params[key]
        return None

    def _get_market_info(self):
        """
        加载合约信息
        :return:
        """
        for symbol in self._symbols:
            f = Future.get_future(symbol=symbol)
            self._future_dict[symbol] = f
            self._contract_size_dict[symbol] = f.contract_size
            self._tick_size_dict[symbol] = f.tick_size
        # self._contract_size_dict = DataFactory.get_contract_size_dict(symbols=list(self._symbols),asset_type=global_variable.ASSETTYPE_FUTURE)
        # self._tick_size_dict = DataFactory.get_tick_size_dict(symbols=list(self._symbols),asset_type=global_variable.ASSETTYPE_FUTURE)

    @abstractclassmethod
    @runing_time
    def _get_history(self,startDate=None,endDate=None,**kwargs):
        """
        加载历史数据（回测/实盘）
        :param startDate:
        :param endDate:
        :return:
        """
        if 'freq_list' in kwargs:
            for freq in kwargs['freq_list']:
                self._market_data_dict[freq] = DataFactory.get_market_data(asset_type=global_variable.ASSETTYPE_FUTURE, freq=freq,
                                                                  symbols=list(self._symbols), start_date=startDate,
                                                                  end_date=endDate)

    # def _prepare_test(self,startDate,endDate,**kwargs):
    #     self._get_market_info()
    #     self._get_history(startDate=startDate,endDate=endDate)

    @abstractclassmethod
    @runing_time
    def run_test(self,startDate,endDate,**kwargs):
        """
        执行回测
        :param startDate:
        :param endDate:
        :param kwargs:
        :return:
        """
        pass

    def gen_config_if_main_contract_switch(self,product_obj,curr_date_str,position_trd, only_close_last_constract=False):
        is_max_volume_symbol_changed,change_info = product_obj.is_max_volume_symbol_changed()
        if not is_max_volume_symbol_changed:
            return []
        last_max_symbol = change_info['last_max_symbol']
        fut = product_obj.get_future(symbol=last_max_symbol)


        if not product_obj.has_night_trading():
            tradeStartTime = fut.tradeDayStartTime()
        else:
            tradeStartTime = fut.tradeNightStartTime()
        valideStartTime = curr_date_str + ' ' + tradeStartTime
        config = self.gen_target_position_config(requestType='Create',
                                                 instrument=fut.ctp_symbol,
                                                 market=fut.market,
                                                 aggToken=self.aggToken,
                                                 requestTime=valideStartTime,
                                                 aggregateRequest='true',
                                                 targetPosition=0,
                                                 strategy=self._strategy_name,
                                                 histLastSignalTime=fut.current_hq_datetime(),
                                                 initiator='Agg-Proxy')
        if only_close_last_constract:
            return [config]
        max_symbol = change_info['max_symbol']
        fut2 = product_obj.get_future(symbol=max_symbol)
        config2 = self.gen_target_position_config(requestType='Create',
                                                  instrument=fut2.ctp_symbol,
                                                  market=fut2.market,
                                                  aggToken=self.aggToken,
                                                  requestTime=valideStartTime,
                                                  aggregateRequest='true',
                                                  targetPosition=position_trd,
                                                  strategy=self._strategy_name,
                                                  histLastSignalTime=fut2.current_hq_datetime(),
                                                  initiator='Agg-Proxy')
        return [config, config2]

    def upload_dailyreturn(self,daily_return,fromdate=None,todate=None,**kwargs):
        if fromdate is None:
            fromdate = self.enddate_of_aggtoken_daily_returns(aggtoken=self.aggToken)
        pf_strategy.upload_dailyreturn(self,aggtoken=self.aggToken,daily_return=daily_return,fromdate=fromdate,todate=todate)

    def upload_volatility(self,daily_return,rolling_window = 252,fromdate=None,todate=None,**kwargs):
        if fromdate is None:
            fromdate = self.enddate_of_aggtoken_daily_returns(aggtoken=self.aggToken)
        pf_strategy.upload_volatility(self,aggtoken=self.aggToken,daily_return=daily_return,rolling_window=rolling_window,fromdate=fromdate,todate=todate)


    def check_config_lines(self,config_day_lines, config_night_lines, stoploss_lines,missing_check=True):
        config_day_lines_result = {}
        list_aggtoken = self.list_aggtoken()
        if config_day_lines is not None:
            for line in config_day_lines:
                pair = line.split(',')
                pair_list = [each.split('=') for each in pair]
                pair_dict = {x: y for (x, y) in pair_list}
                if 'aggToken' in pair_dict and 'targetPosition' in pair_dict and 'strategy' in pair_dict:
                    if pair_dict['strategy'] == self._strategy_name:
                        config_day_lines_result[pair_dict['aggToken']] = 'targetPosition=  %.5f' % numpy.float(pair_dict['targetPosition'])
                if missing_check:
                    for agg in list_aggtoken:
                        if agg not in config_day_lines_result:
                            config_day_lines_result[agg] = 'MISSING !!'

        config_night_lines_result = {}
        if config_night_lines is not None:
            for line in config_night_lines:
                pair = line.split(',')
                pair_list = [each.split('=') for each in pair]
                pair_dict = {x: y for (x, y) in pair_list}
                if 'aggToken' in pair_dict and 'targetPosition' in pair_dict and 'strategy' in pair_dict:
                    if pair_dict['strategy'] == self._strategy_name:
                        config_night_lines_result[pair_dict['aggToken']] = 'targetPosition=  %.5f' % numpy.float(pair_dict['targetPosition'])

        stoploss_lines_result = {}
        if stoploss_lines is not None:
            for line in stoploss_lines:
                pair = line.split(',')
                pair_list = [each.split('=') for each in pair]
                pair_dict = {x: y for (x, y) in pair_list}
                if 'aggToken' in pair_dict and 'targetPosition' in pair_dict and 'strategy' in pair_dict:
                    if pair_dict['strategy'] == self._strategy_name:
                        stoploss_lines_result[pair_dict['aggToken']] = 'targetPosition=  %.5f' % numpy.float(pair_dict['targetPosition'])

        return config_day_lines_result,config_night_lines_result,stoploss_lines_result

    def check_and_mail(self,mail_to_list
                       ,config_day_lines, config_night_lines, stoploss_lines,missing_check=True):
        config_day_lines_result, config_night_lines_result, stoploss_lines_result = self.check_config_lines(config_day_lines, config_night_lines, stoploss_lines,missing_check)
        df = pandas.DataFrame(pandas.Series(config_day_lines_result))
        df.columns = ['info']
        df.index.name = self._strategy_name
        config_day_lines_html = df.to_html()

        df = pandas.DataFrame(pandas.Series(config_night_lines_result))
        df.columns = ['info']
        df.index.name = self._strategy_name
        config_night_lines_html = df.to_html()

        df = pandas.DataFrame(pandas.Series(stoploss_lines_result))
        df.columns = ['info']
        df.index.name = self._strategy_name
        stoploss_lines_html = df.to_html()

        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)

        body_html = '<p>%s</p>' % self._strategy_name + '<p>%s</p>' % hostname  + '<p>%s</p>' % ip

        body_html = body_html + '<p>Config day</p>' + config_day_lines_html
        body_html = body_html + '<p>Config night</p>' + config_night_lines_html
        body_html = body_html + '<p>Stoploss day</p>' + stoploss_lines_html
        ml = mail(host='smtp.mxhichina.com', user='jwliu@jzassetmgmt.com', password='jz2018**', port=25)
        Subject = 'Position info(%s):' % datetime.datetime.now().strftime('%Y%m%d') + self._strategy_name
        ml.send_html(Subject=Subject, mail_to_list=mail_to_list,
                     body_html=body_html)


    @abstractclassmethod
    @runing_time
    def gen_signal(self, **kwargs):
        """
        加工信号
        :param kwargs:
        :return:
        """
        signal_dataframe = None
        return signal_dataframe


    @abstractclassmethod
    @runing_time
    def gen_cofig(self, **kwargs):
        """
        """
        config = Config_base()
        return config

    @abstractclassmethod
    @runing_time
    def run_cofig(self,startDate,endDate,**kwargs):
        """
        """
        pass



