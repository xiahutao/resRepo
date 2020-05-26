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
import datetime
import pytz
import pandas.tseries.holiday as holiday
import data_engine.global_variable as global_variable
from data_engine.data_factory import DataFactory
from analysis.after_trading.strategy import Strategy
from common.devcloud_wrapper import DevCloudWrapper

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
    for strategy_name,strategy_type,account in [('Momentum-Daily',None,'simnow-1'),
                                                ('Trend-Intraday', None, 'simnow-1'),
                                                ('Pair-Intraday','PairStrategy','simnow-1')
                                                ]:
        s = Strategy(strategy_name=strategy_name,strategy_type=strategy_type,account=account)
        # s.get_target_position(start_time=start_time,end_time=end_time)
        # s.get_sys_trade()
        s.get_order_request(start_time=start_time,end_time=end_time)
        # s.mail_compair_order_request_and_back_test_result(mail_to_list=['49680664@qq.com'])
        # time.sleep(10)
        subject = '模拟盘目标仓位(%s-%s):' % (start_time.strftime('%m%d'),end_time.strftime('%m%d')) + s._strategy_name
        #s.mail_trading(mail_to_list=mail_dict[strategy_name], Subject=subject)
        content = s.generate_trading_html()
        DevCloudWrapper.write_document(subject, content)
        #time.sleep(10)