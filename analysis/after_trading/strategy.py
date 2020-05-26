#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/15 16:27
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import pandas
import os
import numpy
import datetime
import pytz
import pandas.tseries.holiday as holiday
from common.mongo_object import mongo_object
import data_engine.global_variable as global_variable
from data_engine.data_factory import DataFactory
from common.mail import mail
from config.config import Config_trading
from data_engine.instrument.future import Future
from data_engine.market_tradingdate import Market_tradingdate

class Strategy(mongo_object):
    def __init__(self,strategy_name,strategy_type,account):
        mongo_object.__init__(self)
        self._strategy_name = strategy_name
        self._strategy_type = strategy_type
        self._account = account

        self._back_test_result = None
        self._order_request = None

        self._order_request_sum = None
        self._sys_trade = None

        self._settle_info = None
        self._settle_info_position_closed = None
        self._settle_info_position_details = None

    def compare_settle_info_and_sys_trade(self):
        settle_info = self._settle_info.reset_index()
        sys_trade = self._sys_trade.reset_index()
        if settle_info is None or sys_trade is None or settle_info.empty or sys_trade.empty:
            return None,None,None,None

        df_pos = pandas.merge(settle_info,sys_trade,how='outer',left_on=['deal_id'],right_on=['exchTradeID'],suffixes=('_settle','_trade'))
        df_pos['diff'] = numpy.round(df_pos['position'] - df_pos['quantity'])
        df_pos_settle_missing = df_pos[df_pos['position'].isna()]
        df_pos_trade_missing = df_pos[df_pos['quantity'].isna()]
        df_pos_or_bt_diff = df_pos[(df_pos['diff'].notna()) & (numpy.abs(df_pos['diff']) > 0.00001)]

        return df_pos_settle_missing,df_pos_trade_missing,df_pos_or_bt_diff,df_pos

    def generate_compare_settle_info_and_sys_trade_html(self):
        df_pos_settle_missing, df_pos_trade_missing, df_pos_or_bt_diff, df_pos = self.compare_settle_info_and_sys_trade()
        body = ''
        if not df_pos_settle_missing.empty:
            body = body + '<p>settle_info: 缺失</p>'
            body = body + Strategy.dataframe_to_html(df_pos_settle_missing.rename(
                columns={'position': 'settle_info', 'quantity': 'sys_trade', 'diff': '差异'}))
        else:
            body = body + '<p>settle_info: 完整</p>'

        if not df_pos_trade_missing.empty:
            body = body + '<p>sys_trade: 缺失</p>'
            body = body + Strategy.dataframe_to_html(df_pos_trade_missing.rename(
                columns={'targetPosition_or': 'settle_info', 'targetPosition_bt': 'sys_trade', 'diff': '差异'}))
        else:
            body = body + '<p>sys_trade: 完整</p>'
        if not df_pos_or_bt_diff.empty:
            body = body + '<p>有效部分: 存在差异</p>'
            body = body + Strategy.dataframe_to_html(df_pos_or_bt_diff.rename(
                columns={'targetPosition_or': 'settle_info', 'targetPosition_bt': 'sys_trade', 'diff': '差异'}))
        else:
            body = body + '<p>有效部分: 一致</p>'

        html_info = '''<!DOCTYPE html>
                    <html lang="en">
                    <head>
                        <meta charset="UTF-8">
                        <title>Title</title>
                    </head>
                    <body><p>%s</p>
                    </body>
                    </html>
                ''' % (''.join([body]))
        return html_info

    def mail_compare_settle_info_and_sys_trade(self,mail_to_list,Subject=None):
        df_pos_settle_missing, df_pos_trade_missing, df_pos_or_bt_diff, df_pos = self.compare_settle_info_and_sys_trade()
        # print(df_pos.index.levels[0])
        if df_pos is None:
            return

        zipfile_list=[]
        zipfile = self._account + '_settle_info_and_sys_trade.csv.gz'
        df_pos.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)
        zipfile = self._account + '_settle_info_missing.csv.gz'
        df_pos_settle_missing.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)
        zipfile = self._account + '_sys_trade_missing.csv.gz'
        df_pos_trade_missing.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)
        zipfile = self._account + '_compare_settle_info_and_sys_trade.csv.gz'
        df_pos_or_bt_diff.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)

        ml = mail(host='smtp.mxhichina.com', user='jwliu@jzassetmgmt.com', password='password*', port=25)
        html_info = self.generate_compare_settle_info_and_sys_trade_html()
        if Subject is None:
            Subject = 'settle_info/sys_trade 比对:' + self._account
        ml.send_html(Subject=Subject, mail_to_list=mail_to_list,
                     body_html=html_info,filename=zipfile_list)
        for each in zipfile_list:
            os.remove(each)

    def get_settle_info_position_closed(self,settle_date =None):
        if settle_date is None:
            settle_date = datetime.datetime.now().strftime('%Y%m%d')
        trading_record = self.mongo_client.get_database('Trading_record')
        cl = trading_record.get_collection('settle_info.PositionClosed')
        cl.ensure_index([('平仓日期',1)])
        result = cl.find({'平仓日期':int(settle_date)}) #
        result_list = list(result[:])
        self._settle_info_position_closed = pandas.DataFrame(result_list)
        if self._settle_info_position_closed is not None and not self._settle_info_position_closed.empty:
            self._settle_info_position_closed = self._settle_info_position_closed.drop(columns=['_id'])\
                .rename(columns = {'买/卖':'buy/short', '交易所':'exchange', '合约':'instrument', '品种':'product', '平仓盈亏':'pnl'
                , '成交价':'price', '开仓日期':'open_date', '平仓日期':'position_date','昨结算':'last_settle','开仓价':'open_price',
                   '手数':'position_v0', '权利金收支':'premium'})
            self._settle_info_position_closed['position'] = self._settle_info_position_closed['position_v0']
            self._settle_info_position_closed.loc[self._settle_info_position_closed['buy/short']=='卖','position'] = -self._settle_info_position_closed.loc[self._settle_info_position_closed['buy/short']=='卖','position']
        pass

    def get_settle_info_position_details(self,settle_date =None):
        if settle_date is None:
            settle_date = datetime.datetime.now().strftime('%Y%m%d')
        trading_record = self.mongo_client.get_database('Trading_record')
        cl = trading_record.get_collection('settle_info.PositionsDetails')
        cl.ensure_index([('position_date',1)])
        result = cl.find({'position_date': settle_date}) #
        result_list = list(result[:])
        self._settle_info_position_details = pandas.DataFrame(result_list)
        if self._settle_info_position_details is not None and not self._settle_info_position_details.empty:
            self._settle_info_position_details = self._settle_info_position_details.drop(columns=['_id'])\
                .rename(columns = {'买/卖':'buy/short', '交易所':'exchange', '合约':'instrument', '品种':'product', '盯市盈亏':'pnl_settle', '浮动盈亏':'pnl_open'
                , '保证金':'margin', '开仓日期':'open_date','昨结算':'last_settle','结算价':'settle','开仓价':'open_price',
                   '持仓量':'position_v0'})
            self._settle_info_position_details['position'] = self._settle_info_position_details['position_v0']
            self._settle_info_position_details.loc[self._settle_info_position_details['buy/short']=='卖','position'] = -self._settle_info_position_details.loc[self._settle_info_position_details['buy/short']=='卖','position']
        pass

    def get_settle_info(self,settle_date =None):
        if settle_date is None:
            settle_date = datetime.datetime.now().strftime('%Y%m%d')
        trading_record = self.mongo_client.get_database('Trading_record')
        cl = trading_record.get_collection('settle_info.trades')
        cl.ensure_index([('成交日期',1)])
        result = cl.find({'成交日期':int(settle_date)}) #
        result_list = list(result[:])
        self._settle_info = pandas.DataFrame(result_list)
        if self._settle_info is not None and not self._settle_info.empty:
            self._settle_info['成交序号'] = self._settle_info['成交序号'].astype(numpy.int)
            self._settle_info = self._settle_info.drop(columns=['_id']).rename(columns = {'买/卖':'buy/short', '交易所':'exchange', '合约':'instrument', '品种':'product', '平仓盈亏':'pnl', '开平':'open_close', '成交价':'price', '成交序号':'deal_id', '成交日期':'date',
                   '成交额':'amount', '手数':'position_v0', '手续费':'fee', '投/保':'position_type', '权利金收支':'premium'}).set_index(['instrument','deal_id']).sort_index()
            self._settle_info['position'] = self._settle_info['position_v0']
            self._settle_info.loc[self._settle_info['buy/short']=='卖','position'] = -self._settle_info.loc[self._settle_info['buy/short']=='卖','position']
        pass

    def _gen_order_request_last_and_sys_trade_last(self,sys_trade_after=None):
        tmp = self._order_request[self._order_request['aggToken'].isna()]
        futures = {}
        for idx in range(len(tmp)):
            row = tmp.iloc[idx]
            strategy = row['strategy']
            if not strategy in ['Momentum-Daily','Ocm-Daily']:
                continue
            instrument = row['instrument']
            if instrument in futures:
                f = futures[instrument]
            else:
                f = Future(symbol=instrument,by_ctp_instrument=True)
                futures[instrument] = f
            self._order_request.loc[row.name,'aggToken'] = (f.product_id + '_VOL').upper()

        self._order_request['aggToken'] = self._order_request['aggToken'].fillna('    ')
        order_request_last =  self._order_request.reset_index().sort_values('date_index').groupby(['account','initiator','strategy','aggToken','symbol'])\
            .last().reset_index().sort_values(['symbol'])

        if sys_trade_after is not None:
            sys_trade_after = pandas.to_datetime(sys_trade_after).replace(hour=18,minute=0,second=0,microsecond=0).tz_localize(pytz.timezone(global_variable.DEFAULT_TIMEZONE))
            sys_trade = self._sys_trade.reset_index().sort_values('date_index')
            sys_trade = sys_trade[sys_trade['date_index'] >= sys_trade_after ]
        else:
            sys_trade =  self._sys_trade.reset_index().sort_values('date_index')
        sys_trade['quantity_cumsum'] = sys_trade.groupby(['account', 'initiator', 'market', 'instrument']).cumsum()['quantity']
        sys_trade_last = sys_trade.groupby(['account', 'initiator', 'market', 'instrument']).last().reset_index().sort_values('instrument')
        return order_request_last,sys_trade_last

    def gen_order_request_last_and_sys_trade_last_file(self,fold,file_date_str,sys_trade_after='2020-01-21'):
        order_request_last, sys_trade_last = self._gen_order_request_last_and_sys_trade_last(sys_trade_after=sys_trade_after)

        config_file_name = os.path.join(fold, 'agg_proxy_target_positions_' + file_date_str + '.csv')
        order_request_last = order_request_last[['instrument','market','targetPosition','account','strategy','aggToken','date_index','createTime']]
        append = False
        Config_trading().dump_csv(config_file_name, append=append)
        for i,row in order_request_last.iterrows():
            config = Config_trading()
            for x,y in row.iteritems():
                config.set_config_item(x,y)
            config.dump_csv(config_file=config_file_name,append=append,with_key=False)
            append=True

        config_file_name = os.path.join(fold, 'actual_positions_' + file_date_str + '.csv')
        sys_trade_last = sys_trade_last[['instrument','market','quantity_cumsum','account']]
        append = False
        Config_trading().dump_csv(config_file_name, append=append)
        for i,row in sys_trade_last.iterrows():
            config = Config_trading()
            for x,y in row.iteritems():
                config.set_config_item(x,y)
            config.dump_csv(config_file=config_file_name,append=append,with_key=False)
            append=True

    def get_sys_trade(self,start_time=None,end_time =None):
        trading_record = self.mongo_client.get_database('Trading_record')
        cl = trading_record.get_collection('SYS_TRADES')
        cl.ensure_index([('account',1)])
        result = cl.find({'account':self._account})
        result_list = list(result[:])
        self._sys_trade = pandas.DataFrame(result_list)
        self._sys_trade['price'] = self._sys_trade['price'].astype(numpy.float)
        self._sys_trade['quantity'] = self._sys_trade['quantity'].astype(numpy.int)
        self._sys_trade['exchTradeID'] = self._sys_trade['exchTradeID'].astype(numpy.int)
        self._sys_trade['symbol'] = self._sys_trade['instrument'].str.upper()
        self._sys_trade = self._sys_trade.drop(columns=['_id'])
        self._sys_trade['date_index'] = pandas.DatetimeIndex(pandas.to_datetime(self._sys_trade['createTime'])).tz_localize(global_variable.DEFAULT_TIMEZONE)

        if start_time is not None:
            self._sys_trade = self._sys_trade[self._sys_trade['date_index'] >=start_time]
        if end_time is not None:
            self._sys_trade = self._sys_trade[self._sys_trade['date_index'] <=end_time]

        self._sys_trade.sort_values(['symbol','date_index'],inplace=True)
        self._sys_trade.set_index(['symbol','date_index'],inplace=True)

    def get_target_position(self,start_time=None,end_time =None,use_start_time_as_index=False):
        trading_record = self.mongo_client.get_database('strategy_log')
        cl = trading_record.get_collection('target_position')
        cl.ensure_index([('strategy_type',1)])
        result = cl.find({'strategy_type':self._strategy_type})
        result_list = list(result[:])
        self._back_test_result = pandas.DataFrame(result_list)
        if not self._back_test_result.empty:
            self._back_test_result['targetPosition'] = self._back_test_result['position']
            if use_start_time_as_index and 'start_time' in self._back_test_result.columns:
                self._back_test_result['date_index'] = pandas.DatetimeIndex(self._back_test_result['start_time'],tz=pytz.timezone(global_variable.DEFAULT_TIMEZONE))
            else:
                self._back_test_result['date_index'] = pandas.DatetimeIndex(pandas.to_datetime(self._back_test_result['date_index'])).tz_localize('UTC').tz_convert(global_variable.DEFAULT_TIMEZONE)
            self._back_test_result['contract_id'] = self._back_test_result['cpt_instrument'].str.upper()
            self._back_test_result.sort_values(['contract_id','date_index'],inplace=True)

            if start_time is not None:
                self._back_test_result = self._back_test_result[self._back_test_result['date_index'] >=start_time]
            if end_time is not None:
                self._back_test_result = self._back_test_result[self._back_test_result['date_index'] <=end_time]

            # 忽略在回测bar不存在的时间点
            # time_ignore = [ '10:15:00', '11:30:00', '15:00:00']
            # time_ignore1 = [ '10:15:01', '11:30:01', '15:00:01']
            # self._back_test_result.set_index('date_index', inplace=True)
            # for time0, time1 in zip(time_ignore, time_ignore1):
            #     self._back_test_result = self._back_test_result.between_time(time1, time0, include_start=True,
            #                                                            include_end=False)
            # self._back_test_result.reset_index(inplace=True)

            self._back_test_result.set_index(['contract_id','date_index'],inplace=True)

    def get_order_request(self,start_time=None,end_time =None, adjust_requestTime = False):
        trading_record = self.mongo_client.get_database('Trading_record')
        cl = trading_record.get_collection('ORDER_REQUESTS')
        cl.ensure_index([('account',1),('strategy',1)])

        query = {}
        if self._strategy_name is not None:
            query['strategy'] = self._strategy_name
        if self._account is not None:
            query['account'] = self._account
        result = cl.find(query)
        result_list = [x for x in result]
        #result_list = list(result[:])
        self._order_request = pandas.DataFrame(result_list)

        self._order_request['createTime'] = pandas.DatetimeIndex(pandas.to_datetime(self._order_request['createTime'])).tz_localize(global_variable.DEFAULT_TIMEZONE)

        # self._order_request['aggToken'] = self._order_request['aggToken'].fillna('    ')
        if start_time is not None:
            self._order_request = self._order_request[self._order_request['createTime'] >=start_time]
        if end_time is not None:
            self._order_request = self._order_request[self._order_request['createTime'] <=end_time]

        self._order_request['targetPosition'] = self._order_request['targetPosition'].astype(numpy.float)

        if adjust_requestTime:
            #模拟盘在09:00:00 位置的交易信号， 对应回测盘的前一个交易日最后时点（todo 特殊节假日前取消夜盘问题需要处理）
            tmp = self._order_request[ self._order_request['requestTime'].str.contains(' 09:00:00')].sort_values('instrument')
            future_by_instrument = {}
            mrk_date = Market_tradingdate(ExchangeID='SHE')
            if not tmp.empty:
                for idx in range(len(tmp)):
                    row = tmp.iloc[idx]
                    instrument = row['instrument']
                    createTime = row['createTime']
                    requestTime = row['requestTime']
                    if instrument in future_by_instrument:
                        f = future_by_instrument[instrument]
                    else:
                        f = Future(symbol=instrument,by_ctp_instrument=True)
                        future_by_instrument[instrument] = f
                    tss = f.get_trading_sessions(bydate=createTime)

                    last_date = mrk_date.get_last_trading_date(pandas.to_datetime(requestTime))
                    last_date_str = pandas.to_datetime(last_date).strftime('%Y-%m-%d')
                    if tss['Session4_Start'] == tss['Session4_End']:
                        end_sesstion = tss['Session3_End']
                    else:
                        end_sesstion = tss['Session4_End']
                    if end_sesstion in ['23:00:00','15:00:00']:
                        self._order_request.loc[row.name, 'requestTime'] = ' '.join([last_date_str, end_sesstion])
                    else:
                        print(createTime,requestTime,end_sesstion,last_date_str)
                        pass
                    # self._order_request.loc[row.name,'requestTime'] = self._order_request.loc[row.name,'requestTime'].replace('09:00:00',instrument_to_end_sesstion[instrument])
                    pass


            #模拟盘在10:30:00 13:30:00 21:00:00 位置的交易信号， 对应回测盘的10:15:00 11:30:00 15:00:00
            for oldstr,newstr in {' 10:30:00':' 10:15:00',' 13:30:00':' 11:30:00',' 21:00:00':' 15:00:00'}.items():
                self._order_request['requestTime'] = self._order_request['requestTime'].str.replace(oldstr,newstr)
        self._order_request['date_index'] = pandas.to_datetime(self._order_request['requestTime'])
        self._order_request['date_index'] = pandas.DatetimeIndex(pandas.to_datetime(self._order_request['requestTime'])).tz_localize(global_variable.DEFAULT_TIMEZONE)


        self._order_request['updateTime'] = pandas.to_datetime(self._order_request['updateTime'])
        self._order_request['symbol'] = self._order_request['instrument'].str.upper()
        self._order_request = self._order_request.drop(columns=['_id'])
        if 'aggToken' not in self._order_request.columns:
            self._order_request['aggToken'] = self._strategy_name #self._order_request['symbol']
        if 'errorMessage' not in self._order_request.columns:
            self._order_request['errorMessage'] = numpy.nan

        #忽略在回测bar不存在的时间点
        # time_ignore= ['21:00:00','23:35:00','10:30:00','13:30:00','09:00:00']
        # time_ignore1= ['21:00:01','23:35:01','10:30:01','13:30:01','09:00:01']
        # self._order_request.set_index('date_index',inplace=True)
        # for time0,time1 in zip(time_ignore,time_ignore1):
        #     self._order_request = self._order_request.between_time(time1,time0,include_start=True,include_end=False)
        # self._order_request.reset_index(inplace=True)

        self._order_request.sort_values(['symbol','date_index'],inplace=True)
        self._order_request.set_index(['symbol','date_index'],inplace=True)

    def order_request_to_target_position(self,from_time=None):
        _order_request = self._order_request.reset_index()[['symbol','date_index','createTime','strategy','targetPosition']]#.groupby(['symbol','date_index']).sum()#[['targetPosition']]
        if from_time is not None:
            _order_request = _order_request[_order_request['createTime'] >= from_time]
        _order_request_list = {strategy : df.set_index(['date_index','symbol']).rename(columns={'targetPosition':strategy})[[strategy]] for strategy,df in _order_request.groupby('strategy')}
        _order_request_list = {strategy: df.reset_index().groupby(['date_index','symbol']).sum() for strategy,df in _order_request_list.items() }
        ret_order_request = None
        for _,df in _order_request_list.items():
            if ret_order_request is None:
                ret_order_request = df
                continue
            ret_order_request = pandas.merge(ret_order_request,df,how='outer',left_index=True,right_index=True)
        ret_order_request = ret_order_request.reset_index().sort_values(['symbol','date_index'])
        for strategy in _order_request_list.keys():
            ret_order_request[strategy] = ret_order_request.groupby('symbol').ffill()[strategy]
        ret_order_request['targetPosition'] = ret_order_request[_order_request_list.keys()].sum(axis=1)
        ret_order_request = ret_order_request.set_index('date_index')
        if ret_order_request.index.tzinfo is None:
            ret_order_request.index = pandas.DatetimeIndex(pandas.to_datetime(ret_order_request.index)).tz_localize('UTC').tz_convert(global_variable.DEFAULT_TIMEZONE)

        return ret_order_request

    def group_by_aggToken(self):
        if self._order_request is None:
            return None
        return {x:y for x,y in self._order_request.groupby('aggToken')}

    def compare_order_request_and_back_test_result(self):
        if self._back_test_result is None or self._back_test_result.empty:
            return None,None,None,None
        if self._order_request is None or self._order_request.empty:
            return None,None,None,None

        if self._strategy_type is None:
            self._order_request['strategy_id'] = self._order_request['aggToken']
        else:
            self._order_request['strategy_id'] = self._strategy_type + '_' + self._order_request['aggToken']
        order_request = self._order_request.reset_index()
        order_request['contract_id'] = order_request['symbol']
        order_request = order_request.set_index(['strategy_id','contract_id','date_index']).sort_index()
        back_test_result = self._back_test_result.reset_index().set_index(['strategy_id','contract_id','date_index']).sort_index()

        df = pandas.merge(order_request,back_test_result,how='outer',left_index=True,right_index=True,suffixes=('_or','_bt'))
        df_pos = df[['targetPosition_or','targetPosition_bt']]#.ffill()
        df_pos['diff'] = df_pos['targetPosition_or'] - df_pos['targetPosition_bt']
        df_pos_or_missing = df_pos[df_pos['targetPosition_or'].isna()]
        df_pos_bt_missing = df_pos[df_pos['targetPosition_bt'].isna()]



        df_pos_or_bt_diff = df_pos[(df_pos['diff'].notna()) & (numpy.abs(df_pos['diff']) > 0.0001)]

        return df_pos_or_missing,df_pos_bt_missing,df_pos_or_bt_diff,df_pos

    @staticmethod
    def dataframe_to_html(df,only_20_row=True):
        if df is None or df.empty:
            return ''
        if len(df)< 20 or not only_20_row:
            return '<p>' + df.to_html(float_format=lambda x: format(x, ',.5f')) + '</p>'
        else:
            return '<p>' + df.head(5).to_html(float_format=lambda x: format(x, ',.5f')) + '</p>' + '<p>......</p>' + '<p>' + df.tail(10).to_html(float_format=lambda x: format(x, ',.5f')) + '</p>'


    def generate_compare_order_request_and_back_test_result_html(self):
        df_pos_or_missing, df_pos_bt_missing, df_pos_or_bt_diff,df_pos = self.compare_order_request_and_back_test_result()
        body = ''
        if not df_pos_or_missing.empty:
            body = body + '<p>模拟盘目标仓位: 缺失</p>'
            body = body + Strategy.dataframe_to_html(df_pos_or_missing.rename(
                columns={'targetPosition_or': '模拟盘', 'targetPosition_bt': '回测盘', 'diff': '差异'}))
        else:
            body = body + '<p>模拟盘目标仓位: 完整</p>'

        if not df_pos_bt_missing.empty:
            body = body + '<p>回测盘目标仓位: 缺失</p>'
            body = body + Strategy.dataframe_to_html(df_pos_bt_missing.rename(
                columns={'targetPosition_or': '模拟盘', 'targetPosition_bt': '回测盘', 'diff': '差异'}))
        else:
            body = body + '<p>回测盘目标仓位: 完整</p>'
        if not df_pos_or_bt_diff.empty:
            body = body + '<p>有效目标仓位部分: 存在差异</p>'
            body = body + Strategy.dataframe_to_html(df_pos_or_bt_diff.rename(
                columns={'targetPosition_or': '模拟盘', 'targetPosition_bt': '回测盘', 'diff': '差异'}))
        else:
            body = body + '<p>有效目标仓位部分: 一致</p>'

        html_info = '''<!DOCTYPE html>
                    <html lang="en">
                    <head>
                        <meta charset="UTF-8">
                        <title>Title</title>
                    </head>
                    <body><p>%s</p>
                    </body>
                    </html>
                ''' % (''.join([body]))
        return html_info


    def mail_compare_order_request_and_back_test_result(self,mail_to_list,Subject=None):
        df_pos_or_missing, df_pos_bt_missing, df_pos_or_bt_diff,df_pos = self.compare_order_request_and_back_test_result()
        # print(df_pos.index.levels[0])
        if df_pos is None:
            return

        zipfile_list=[]
        zipfile = self._strategy_name + '_order_request_and_back_test_result.csv.gz'
        df_pos.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)
        zipfile = self._strategy_name + '_order_request_missing.csv.gz'
        df_pos_or_missing.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)
        zipfile = self._strategy_name + '_back_test_result_missing.csv.gz'
        df_pos_bt_missing.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)
        zipfile = self._strategy_name + '_compair_order_request_and_back_test_result.csv.gz'
        df_pos_or_bt_diff.to_csv(zipfile, compression='gzip')
        zipfile_list.append(zipfile)

        ml = mail(host='smtp.mxhichina.com', user='jwliu@jzassetmgmt.com', password='password*', port=25)
        html_info = self.generate_compare_order_request_and_back_test_result_html()
        if Subject is None:
            Subject = '模拟盘/回测盘 目标仓位比对:' + self._strategy_name
        ml.send_html(Subject=Subject, mail_to_list=mail_to_list,
                     body_html=html_info,filename=zipfile_list)
        for each in zipfile_list:
            os.remove(each)

    def check_sys_trade_and_order_request(self):
        self._order_request_sum = self._order_request.groupby(['symbol','date_index']).sum()
        self._order_request_sum['quantity'] = numpy.round(self._order_request_sum['targetPosition'])
        df = pandas.merge(self._sys_trade, self._order_request_sum,how='outer',left_index=True,right_index=True,suffixes=('_st','_or'))
        return df.sort_index()

    def generate_trading_html(self):
        tmp = self.group_by_aggToken()
        body_list = []
        for aggToken, df in tmp.items():
            if df.empty:
                continue
            body = '<p>' + aggToken + '</p>'
            body = body + '<p>' + Strategy.dataframe_to_html(
                df[['account', 'strategy', 'targetPosition', 'initiator', 'errorMessage']]) + '</p>'
            body_list.append(body)
        html_info = '''<!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Title</title>
            </head>
            <body><p>%s</p>
            </body>
            </html>
        ''' % (''.join(body_list))
        return html_info

    def mail_trading(self,mail_to_list,Subject=None):
        tmp = self.group_by_aggToken()
        #ml = mail(host='smtp.mxhichina.com', user='jwliu@jzassetmgmt.com', password='password*', port=25)
        if len(tmp)>0:
            zipfile = self._strategy_name + '_order_request.csv.gz'
            self._order_request.to_csv(zipfile, compression='gzip')
            html_info = self.generate_trading_html()
            print(html_info)
            if Subject is None:
                Subject = '模拟盘目标仓位:' + self._strategy_name
            #ml.send_html(Subject=Subject, mail_to_list=mail_to_list,
            #            body_html=html_info,filename=zipfile)
            os.remove(zipfile)
#
if __name__ == '__main__':
    import time
    i = 0
    end_time = (datetime.datetime.now().replace(hour=18,minute=0,second=0,microsecond=0)+datetime.timedelta(-i)).astimezone(pytz.timezone(global_variable.DEFAULT_TIMEZONE))
    start_time = holiday.previous_friday(end_time + datetime.timedelta(-1))
#     while True:
#         if i> 10:
#             break
#         i += 1
    DataFactory.config('password',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)
    for strategy_name,strategy_type,account in [(None,None,'simnow-1')]:
        s = Strategy(strategy_name=strategy_name,strategy_type=strategy_type,account=account)
        s.get_order_request(start_time=start_time,end_time=end_time)
        print(s._order_request)
#             s.get_sys_trade()
#             order_request_last, sys_trade_last = s._gen_order_request_last_and_sys_trade_last()
#
#             order_request_last = order_request_last[['instrument','market','targetPosition','account','strategy','aggToken']]
#             s.gen_order_request_last_and_sys_trade_last_file(fold=r'e:',file_date_str=end_time.strftime('%Y%m%d'))
#             pass
#         end_time = start_time
#         start_time = holiday.previous_friday(end_time + datetime.timedelta(-1))
#         s.get_settle_info('20200116')
#         s.get_sys_trade(start_time=start_time,end_time=end_time)
#
#         s.mail_compare_settle_info_and_sys_trade(mail_to_list=['49680664@qq.com'])
#         break
        # s.get_target_position(start_time=start_time,end_time=end_time)
        # s.get_order_request(start_time=start_time,end_time=end_time)
        # s.mail_compair_order_request_and_back_test_result(mail_to_list=['49680664@qq.com'])
        # time.sleep(10)
        # s.mail_trading(mail_to_list=['49680664@qq.com'])
        # time.sleep(10)

    # df = s.check_sys_trade_and_order_request()
    # df.to_csv('sys_trade_vs_order_request.csv')