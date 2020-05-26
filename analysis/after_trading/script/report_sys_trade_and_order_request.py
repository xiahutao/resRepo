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
import time
import pandas.tseries.holiday as holiday
import data_engine.global_variable as global_variable
from data_engine.data_factory import DataFactory
from analysis.after_trading.strategy import Strategy

if __name__ == '__main__':
    end_time = datetime.datetime.now().replace(hour=16,minute=0,second=0,microsecond=0).astimezone(pytz.timezone(global_variable.DEFAULT_TIMEZONE))
    start_time = holiday.previous_friday(end_time + datetime.timedelta(-1))
    # ['Momentum-Daily', 'Pair-Intraday', 'Trend-Intraday']

    DataFactory.config('password',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)

    mail_dict = {
        'Momentum-Daily':['519518384@qq.com','49680664@qq.com'],
        'Pair-Intraday':['519518384@qq.com','49680664@qq.com'],
        'Trend-Intraday':['519518384@qq.com','49680664@qq.com']
    }
    s = Strategy(strategy_name=None, strategy_type=None, account='simnow-1')
    # s.get_target_position(start_time=start_time,end_time=end_time)
    s.get_sys_trade()
    # s.get_order_request(start_time=start_time,end_time=end_time)

    # df = s.check_sys_trade_and_order_request()
    # df.to_csv('sys_trade_vs_order_request.csv')
    import pandas
    pandas.DataFrame()
    sys_trade = s._sys_trade.reset_index().sort_values('createTime')
    sys_trade['quantity_cumsum'] = sys_trade.groupby(['account','initiator','market','instrument']).cumsum()['quantity']

    sys_trade.sort_values(['symbol', 'date_index'], inplace=True)
    sys_trade.set_index(['symbol', 'date_index'], inplace=True)
    print(s._sys_trade)
