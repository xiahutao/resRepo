#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020/1/16 15:11
# @Author  : jwliu
# @Site    :
# @Software: PyCharm

import sys
import os
CurrentPath = os.path.dirname(__file__)
sys.path.append(CurrentPath.replace(r'after_trading/script', ''))
import pymongo
import datetime
import pytz
import pandas
import numpy
import time
from common.mail import mail
import pandas.tseries.holiday as holiday
import data_engine.global_variable as global_variable
from data_engine.data_factory import DataFactory
from analysis.after_trading.strategy import Strategy
from data_engine.instrument.future import Future
from execution.execution import Execution
from settlement.settlement import Settlement
from analysis.analysis import Analysis_ex
from common.devcloud_wrapper import DevCloudWrapper
from data_engine.market_tradingdate import Market_tradingdate

if __name__ == '__main__':
    DataFactory.config('password',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)
    mkt =  Market_tradingdate(ExchangeID='SHE')
    from_time = datetime.datetime(2020,2,5,8,0,0,0,tzinfo=pytz.timezone(global_variable.DEFAULT_TIMEZONE))
    i = 1

    #这里在比较end_time之前24小时内， 模拟盘产生的目标持仓信号， 转换成策略持仓信号dataframe， 进行撮合清算， 与模拟盘的结算单盈亏进行核对比较；
    #end_time 取14：59：59， 模拟盘在15：00：00的交易，会在下个交易日开盘执行
    end_time = (datetime.datetime.now().replace(hour=14, minute=59, second=59, microsecond=0) + datetime.timedelta(
        -i)).astimezone(pytz.timezone(global_variable.DEFAULT_TIMEZONE))
    end_time = mkt.get_last_trading_date(date=end_time.strftime('%Y%m%d'),include_current_date=True)
    end_time = pandas.to_datetime(end_time.replace(hour=14, minute=59, second=59, microsecond=0)).tz_localize(global_variable.DEFAULT_TIMEZONE)
    start_time_1 = mkt.get_last_trading_date(date=end_time.strftime('%Y%m%d'),include_current_date=False)
    start_time_1 = pandas.to_datetime(start_time_1.replace(hour=14, minute=59, second=59, microsecond=0)).tz_localize(global_variable.DEFAULT_TIMEZONE)
    # start_time_1 = holiday.previous_friday(end_time + datetime.timedelta(-1)).replace(hour=14, minute=59, second=59, microsecond=0)
    #
    start_time = holiday.previous_friday(end_time + datetime.timedelta(-20)).replace(hour=14, minute=59, second=59, microsecond=0)


    for strategy_name, strategy_type, account in [(None, None, 'simnow-1')]:
        s = Strategy(strategy_name=strategy_name, strategy_type=strategy_type, account=account)
        s.get_order_request(start_time=start_time, end_time=end_time,adjust_requestTime=True)
        s.get_sys_trade(start_time=start_time, end_time=end_time)
        s.get_settle_info(settle_date= end_time.strftime('%Y%m%d') )
        s.get_settle_info_position_closed(settle_date= end_time.strftime('%Y%m%d'))
        s.get_settle_info_position_details(settle_date= end_time.strftime('%Y%m%d'))

        #模拟盘成交记录，从start_time到end_time期间
        settle_info_trade = s._settle_info.reset_index()
        sys_trade = s._sys_trade.reset_index()
        sys_trade['trade_id'] = sys_trade['exchTradeID'].astype(numpy.int64)
        settle_info_trade['trade_id'] = settle_info_trade['deal_id'].astype(numpy.int64)
        sys_trade['contract_id'] = sys_trade['symbol']
        sys_trade['contract_size'] = None
        symbols = list(sys_trade['instrument'].unique())
        for symbol in symbols:
            f = Future(symbol=symbol, by_ctp_instrument=True)
            sys_trade.loc[sys_trade['instrument'] == symbol, 'contract_id'] = f.jz_symbol
            sys_trade.loc[sys_trade['instrument'] == symbol, 'contract_size'] = f.contract_size
        sys_trade = sys_trade[(sys_trade['date_index'] >= start_time_1) & (sys_trade['date_index'] <= end_time) ].sort_values(['symbol','date_index','quantity'])
        #同品种按时间的成交序号，用于与回测系统merge
        sys_trade['num'] = 1
        sys_trade['num'] = sys_trade.groupby('symbol').cumsum()['num']
        sys_trade = pandas.merge(sys_trade,settle_info_trade[['instrument','trade_id','fee']],how='left',left_on=['instrument','trade_id'],right_on=['instrument','trade_id'])

        #模拟盘的order_request生成目标持仓文件
        signal_dataframe = s.order_request_to_target_position(from_time)
        signal_dataframe = signal_dataframe[ signal_dataframe.index <= end_time]   # 剔除15：00：00的持仓，避免15：00：00的成交

        # signal_dataframe = signal_dataframe[ signal_dataframe.index >=  from_time]

        signal_dataframe['contract_id'] = signal_dataframe['symbol']
        signal_dataframe['cpt_instrument'] = signal_dataframe['symbol']
        symbols = list(signal_dataframe['symbol'].unique())
        for symbol in symbols:
            f = Future(symbol=symbol, by_ctp_instrument=True)
            signal_dataframe.loc[signal_dataframe['symbol'] == symbol, 'contract_id'] = f.jz_symbol
            signal_dataframe.loc[signal_dataframe['symbol'] == symbol, 'cpt_instrument'] = f.ctp_symbol
        signal_dataframe['symbol'] = signal_dataframe['contract_id']
        signal_dataframe['asset_type'] = global_variable.ASSETTYPE_FUTURE
        signal_dataframe['position'] = numpy.round(signal_dataframe['targetPosition'])
        signal_dataframe['freq'] = global_variable.FREQ_5M

        # signal_dataframe.to_csv(r'e:\signal_dataframe.csv')
        # s._order_request.to_csv(r'e:\order_request.csv')
        # s._sys_trade.to_csv(r'e:\sys_trade.csv')
        #撮合， 使用5m数据，当前bar的收盘价撮合。  因使用order_request的request_time生产的目标持仓文件， 所以需要用当前bar的close
        success, target_position = Execution(freq=global_variable.FREQ_5M, exec_lag=0).exec_trading(
            signal_dataframe=signal_dataframe)
        if success:
            settle_obj = Settlement()
            settle_obj.settle(positions_dataframe=target_position,bidAskDollar=0)  # 去掉bidAskDollar
            _positions_daily_dataframe = settle_obj._positions_daily_dataframe.reset_index()

            transactions_dataframe = settle_obj._transactions_dataframe.reset_index()
            transactions_dataframe['symbol'] = transactions_dataframe['cpt_instrument'].str.upper()
            transactions_dataframe = transactions_dataframe[
                (transactions_dataframe['date_index'] >= start_time_1) & (transactions_dataframe['date_index'] <= end_time)].sort_values(
                ['symbol', 'date_index', 'transactions'])
            #同品种按时间的成交序号，用于与回测系统merge
            transactions_dataframe['num'] = 1
            transactions_dataframe['num'] = transactions_dataframe.groupby('symbol').cumsum()['num']

            #成交差异部分
            sys_trade_diff = pandas.merge(sys_trade[['date_index','symbol','contract_id','quantity','price','contract_size','fee','num']],transactions_dataframe[['date_index','symbol','contract_id','transactions','transaction_price','contract_size','settle','fee','num']],
                         how='outer',left_on=['symbol','num'],right_on=['symbol','num'],suffixes=('_st','_bt'))
            sys_trade_diff['transaction_price_2'] = sys_trade_diff['transaction_price']
            #回测撮合中前一日15:00:00成交的交易（模拟盘在今天开盘成交）， 交易价格改为昨天的结算价（当作昨天的日终持仓，计算今天pnl）
            sys_trade_diff.loc[ sys_trade_diff['date_index_bt'] < start_time_1 + datetime.timedelta(hours=1),'transaction_price_2' ] = sys_trade_diff.loc[ sys_trade_diff['date_index_bt'] < start_time_1 + datetime.timedelta(hours=1),'settle' ]

            #撮合价格和模拟盘成交价格之间差异的影响部分
            sys_trade_diff['price_diff'] = sys_trade_diff['price'] - sys_trade_diff['transaction_price_2']
            sys_trade_diff['quantity_diff'] = sys_trade_diff['quantity'] - sys_trade_diff['transactions']

            # _positions_daily_dataframe = _positions_daily_dataframe.groupby(['datetime_index','symbol']).sum()

            _transactions_dataframe = settle_obj._transactions_dataframe.reset_index()
            _transactions_dataframe = _transactions_dataframe[ _transactions_dataframe['trading_date'] <= end_time  + datetime.timedelta(hours=3) ]
            _transactions_dataframe = _transactions_dataframe[ _transactions_dataframe['trading_date'] >= end_time + datetime.timedelta(hours=-3) ]

            _positions_daily_dataframe = _positions_daily_dataframe[ _positions_daily_dataframe['datetime_index'] <= end_time + datetime.timedelta(hours=3) ]
            _positions_daily_dataframe = _positions_daily_dataframe[ _positions_daily_dataframe['datetime_index'] >= end_time + datetime.timedelta(hours=-3) ]


            #模拟盘结算单部分
            if s._settle_info is not None and not s._settle_info.empty:
                settle_obj_trade = s._settle_info.reset_index().groupby(['instrument', 'position_date']).sum()
            settle_info_position_closed = s._settle_info_position_closed.reset_index()
            settle_info_position_details = s._settle_info_position_details.reset_index()

            settle_info_position_closed['contract_size'] = None
            symbol_by_instrument = {}
            symbols = list(settle_info_position_closed['instrument'].unique()) + list(
                settle_info_position_details['instrument'].unique())
            for instrument in set(symbols):
                f = Future(symbol=instrument, by_ctp_instrument=True)
                settle_info_position_closed.loc[
                    settle_info_position_closed['instrument'] == instrument, 'contract_size'] = f.contract_size
                symbol_by_instrument[f.jz_symbol] = instrument
            pass
            settle_info_position_closed['pnl_settle'] = (settle_info_position_closed['price'] -
                                                         settle_info_position_closed['last_settle']) * \
                                                        -settle_info_position_closed['position'] * \
                                                        settle_info_position_closed['contract_size']
            settle_info_position_closed.loc[ settle_info_position_closed['open_date'] == int(end_time.strftime('%Y%m%d')), 'pnl_settle'] = \
                settle_info_position_closed.loc[ settle_info_position_closed['open_date'] == int(end_time.strftime('%Y%m%d')), 'pnl']

            settle_info_position_closed_sum = settle_info_position_closed.groupby(['instrument']).sum()  # [['pnl_settle']]
            settle_info_position_details_sum = settle_info_position_details.groupby(['instrument']).sum()  # [['pnl_settle']]

            settle_info_pnl_settle_sum = pandas.merge(settle_info_position_closed_sum[['pnl_settle']]
                                                      , settle_info_position_details_sum[['pnl_settle']],
                                                      left_index=True, right_index=True, how='outer',
                                                      suffixes=('_pclosed', '_pdetails')).fillna(0)

            settle_info_pnl_settle_sum['symbol'] = None
            for symbol, instrument in symbol_by_instrument.items():
                settle_info_pnl_settle_sum.loc[instrument, 'symbol'] = symbol

            #模拟盘结算单部分的pnl,
            settle_info_pnl_settle_sum['pnl'] = settle_info_pnl_settle_sum['pnl_settle_pclosed'] + \
                                                settle_info_pnl_settle_sum['pnl_settle_pdetails']
            # 结算单上的持仓明细， 平仓交易中的盯市盈亏。持仓明细的结算-昨结算， 平仓交易的平仓价 - 昨结算
            settle_info_pnl = settle_info_pnl_settle_sum.set_index('symbol')

            #模拟盘pnl， 与分析模块pnl差异
            pnl_compare = pandas.merge(_positions_daily_dataframe[['symbol','daily_pnl_gross_settle','daily_pnl_settle','daily_pnl_fee_settle']],settle_info_pnl.reset_index(),how='outer',left_on='symbol',right_on='symbol')
            pnl_compare['diff'] = pnl_compare['daily_pnl_gross_settle'] - pnl_compare['pnl']
            pnl_compare = pnl_compare.set_index('symbol')

            #计算成交价格差异导致的pnl差异部分
            sys_trade_diff = sys_trade_diff.reset_index()
            sys_trade_diff['amount_st'] = sys_trade_diff['quantity'] * sys_trade_diff['price'] * sys_trade_diff['contract_size_st']
            sys_trade_diff['amount_bt'] = sys_trade_diff['transactions'] * sys_trade_diff['transaction_price_2'] * sys_trade_diff['contract_size_bt']
            sys_trade_diff_sumdiff_st = sys_trade_diff.groupby('contract_id_st').sum()[['amount_st']]
            sys_trade_diff_sumdiff_bt = sys_trade_diff.groupby('contract_id_bt').sum()[['amount_bt']]
            sys_trade_diff_sumdiff = pandas.merge(sys_trade_diff_sumdiff_st,sys_trade_diff_sumdiff_bt,how='outer',left_index=True,right_index=True)
            sys_trade_diff_sumdiff.fillna(0,inplace=True)
            sys_trade_diff_sumdiff['sumdiff'] = sys_trade_diff_sumdiff['amount_st'] - sys_trade_diff_sumdiff['amount_bt']
            #target_position

            # 模拟盘pnl， 与分析模块pnl差异(剔除成交价格差异部分后) ， diff_ex为0表示无差异
            pnl_compare_ex = pandas.merge(pnl_compare,sys_trade_diff_sumdiff[['sumdiff']], how='outer',left_index=True,right_index=True)
            pnl_compare_ex['diff_ex'] = pnl_compare_ex['diff'].fillna(0) - pnl_compare_ex['sumdiff'].fillna(0)




            pnl_compare_report = pnl_compare_ex[['daily_pnl_gross_settle','pnl','sumdiff','diff_ex']].rename(columns={'daily_pnl_gross_settle':'回测pnl','pnl':'结算单pnl','sumdiff':'成交价格影响','diff_ex':'剩余差异pnl'})
            pnl_compare_report.index.name = '_'.join([account,'pnl 检查'])
            sys_trade_diff_report = sys_trade_diff[['date_index_st','contract_id_st','quantity','price','date_index_bt','contract_id_bt','transactions','transaction_price_2','price_diff','quantity_diff','fee_st','fee_bt']]

            title = '_'.join([account,'pnl 检查']) + '(%s-%s):' % (start_time_1.strftime('%m%d'), end_time.strftime('%m%d'))
            body = ''

            zipfile_list = []
            # zipfile = 'pnl_compare_report.csv.gz'
            # pnl_compare_report.to_csv(zipfile, compression='gzip')
            # zipfile_list.append(zipfile)
            # zipfile = 'sys_trade_diff_report.csv.gz'
            # sys_trade_diff_report.to_csv(zipfile, compression='gzip')
            # zipfile_list.append(zipfile)

            pnl_compare_report_tmp = pnl_compare_ex[ numpy.abs(pnl_compare_ex['diff_ex']) > 0.0001]

            if not pnl_compare_report.empty:
                if pnl_compare_report_tmp.empty:
                    body = body + '<p>pnl 无差异</p>'
                else:
                    body = body + '<p>pnl 差异: 注意(%s)</p>' % ','.join (pnl_compare_report_tmp.index.unique())
                body = body + Strategy.dataframe_to_html(pnl_compare_report,only_20_row=False)
            else:
                body = body + '<p>检查是否当天没交易！！</p>'

            if not sys_trade_diff_report.empty:
                body = body + '<p>成交 比对</p>'
                body = body + Strategy.dataframe_to_html(sys_trade_diff_report,only_20_row=False)
            else:
                body = body + '<p>检查是否当天没交易！！</p>'


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

            ml = mail(host='smtp.mxhichina.com', user='jwliu@jzassetmgmt.com', password='password*', port=25)
            ml.send_html(Subject=title, mail_to_list=['49680664@qq.com'],
                         body_html=html_info,filename=zipfile_list)
            for each in zipfile_list:
                os.remove(each)
            DevCloudWrapper.write_document(title, html_info)
            pass