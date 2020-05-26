import os
import pandas
import datetime
import pytz
import data_engine.global_variable as global_variable
import pandas.tseries.holiday as holiday
from data_engine.data_factory import DataFactory
from analysis.after_trading.strategy import Strategy
from data_engine.market_tradingdate import Market_tradingdate

if __name__ == '__main__':
    DataFactory.config('password',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)

    mkt =  Market_tradingdate(ExchangeID='SHE')

    end_time = (datetime.datetime.now().replace(hour=18, minute=0, second=0, microsecond=0) + datetime.timedelta(-0)).strftime('%Y%m%d')
    end_time = mkt.get_last_trading_date(date=end_time,include_current_date=True)
    end_time = end_time.replace(hour=18, minute=0, second=0, microsecond=0)

    end_time = pandas.to_datetime(end_time).tz_localize(global_variable.DEFAULT_TIMEZONE)
    start_time = mkt.get_last_trading_date(date=end_time.strftime('%Y%m%d'),include_current_date=False)
    start_time = start_time.replace(hour=18, minute=0, second=0, microsecond=0)
    start_time = pandas.to_datetime(start_time).tz_localize(global_variable.DEFAULT_TIMEZONE)

    from_time = datetime.datetime(2020,2,4,18,0,0,0,tzinfo=pytz.timezone(global_variable.DEFAULT_TIMEZONE))

    next_time = mkt.get_next_trading_date(date=end_time.strftime('%Y%m%d'))
    next_time = next_time.replace(hour=18, minute=0, second=0, microsecond=0)
    next_time = pandas.to_datetime(next_time).tz_localize(global_variable.DEFAULT_TIMEZONE)

    for strategy_name,strategy_type,account in [(None,None,'simnow-1')]:
        s = Strategy(strategy_name=strategy_name,strategy_type=strategy_type,account=account)
        s.get_order_request(start_time=from_time,end_time=end_time,adjust_requestTime=False)
        s.get_sys_trade(start_time=from_time)
        # order_request_last, sys_trade_last = s._gen_order_request_last_and_sys_trade_last()

        # order_request_last = order_request_last[['instrument','market','targetPosition','account','strategy','aggToken']]
        s.gen_order_request_last_and_sys_trade_last_file(fold=r'D:\tradelib\test\positions',file_date_str=next_time.strftime('%Y%m%d'))
        print(next_time,start_time,end_time)
