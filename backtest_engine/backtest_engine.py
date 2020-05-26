#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/6 15:17
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import pandas
from strategy.strategy import Strategy
from config.config import Config_base
from analysis.execution_settle_analysis import execution_settle_analysis


class Backtest_engine(object):
    def __init__(self,config,strategy):
        assert isinstance(config,Config_base)
        assert isinstance(strategy,Strategy)
        self._config = config
        self._strategy = strategy

    def get_history(self,startDate,endDate,**kwargs):
        self._strategy._get_market_info()
        self._strategy._get_history(startDate=startDate,endDate=endDate,**kwargs)
        pass

    def run_test(self,startDate,endDate,**kwargs):
        signal_dataframe = self._strategy.run_test(startDate=startDate,endDate=endDate,**kwargs)
        analysis_obj = execution_settle_analysis(signal_dataframe=signal_dataframe, config=self._config)
        analysis_obj.plot_cumsum_pnl(show=False)
        analysis_obj.save_result(from_date = pandas.to_datetime(startDate), to_date = pandas.to_datetime(endDate))
        pass