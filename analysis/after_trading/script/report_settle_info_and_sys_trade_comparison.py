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
from common.devcloud_wrapper import DevCloudWrapper

if __name__ == '__main__':
    end_time = datetime.datetime.now().replace(hour=18,minute=0,second=0,microsecond=0).astimezone(pytz.timezone(global_variable.DEFAULT_TIMEZONE))
    start_time = holiday.previous_friday(end_time + datetime.timedelta(-1))
    # ['Momentum-Daily', 'Pair-Intraday', 'Trend-Intraday']

    DataFactory.config('password',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)

    s = Strategy(strategy_name=None, strategy_type=None, account='simnow-1')
    s.get_settle_info(end_time.strftime('%Y%m%d'))
    s.get_sys_trade(start_time=start_time, end_time=end_time)
    content = s.generate_compare_settle_info_and_sys_trade_html()
    title = 'settle_info/sys_trade 比对(%s-%s):'  % (start_time.strftime('%m%d'),end_time.strftime('%m%d')) + s._account
    DevCloudWrapper.write_document(title, content)
    #s.mail_compare_settle_info_and_sys_trade(mail_to_list=['49680664@qq.com'],Subject = 'settle_info/sys_trade 比对(%s-%s):'  % (start_time.strftime('%m%d'),end_time.strftime('%m%d')) + s._account)
