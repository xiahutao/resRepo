# -*- coding: utf-8 -*-import sys
import os
import sys
CurrentPath = os.path.dirname(__file__)
# print(CurrentPath)
sys.path.append(CurrentPath.replace('analysis',''))
import pandas
import numpy
import traceback
import datetime

import json
from data_engine.data_factory import DataFactory
from common.file_saver import file_saver
from common.decorator import runing_time
from common.os_func import check_fold
import data_engine.setting as setting
import matplotlib.pyplot as plt
import copy

import pyfolio
from pyfolio.utils import DAILY
from analysis.pyfolio_ex.plotting import plot_slippage_sweep,plot_slippage_sensitivity
from config.config import Config_back_test
from settlement.settlement import Settlement
import empyrical as ep

class Analysis_func(object):

    @staticmethod
    def cut_returns(daily_returns,look_back_start = None,look_back_end=None):
        if daily_returns is None:
            return None
        if look_back_start is not None:
            daily_returns = daily_returns[daily_returns.index >= look_back_start + datetime.timedelta(
                days=1)]  # _look_back_start到_look_back_end 前开后闭
        if look_back_end is not None:
            daily_returns = daily_returns[daily_returns.index <= look_back_end]
        return daily_returns

    @staticmethod
    def max_drawdown(daily_returns,look_back_start = None,look_back_end=None):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.max_drawdown(returns= daily_returns)

    @staticmethod
    def volatility(daily_returns,span = 252,min_periods=20):
        return daily_returns.ewm(span=span, min_periods=min_periods, adjust=False).std(bias=True) * (
                    252 ** 0.5)

    @staticmethod
    def annual_return(daily_returns,look_back_start = None,look_back_end=None,period=DAILY):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.annual_return(returns=daily_returns,period=period)

    @staticmethod
    def annual_volatility(daily_returns,look_back_start = None,look_back_end=None,period=DAILY):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.annual_volatility(returns=daily_returns,period=period)

    @staticmethod
    def calmar_ratio(daily_returns,look_back_start = None,look_back_end=None,period=DAILY):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.calmar_ratio(returns=daily_returns,period=period)

    @staticmethod
    def omega_ratio(daily_returns,look_back_start = None,look_back_end=None,annual_return_threshhold=0.0):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.omega_ratio(returns=daily_returns,annual_return_threshhold=annual_return_threshhold)

    @staticmethod
    def sortino_ratio(daily_returns,look_back_start = None,look_back_end=None,period=DAILY):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.sortino_ratio(returns=daily_returns,period=period)

    @staticmethod
    def sharpe_ratio(daily_returns,look_back_start = None,look_back_end=None, risk_free=0,period=DAILY):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.sharpe_ratio(returns=daily_returns, risk_free=risk_free,period=period)

    @staticmethod
    def cum_returns(daily_returns,look_back_start = None,look_back_end=None, starting_value=0):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.cum_returns(returns=daily_returns, starting_value=starting_value)

    @staticmethod
    def downside_risk(daily_returns,look_back_start = None,look_back_end=None, required_return=0,period=DAILY):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.downside_risk(returns=daily_returns, required_return=required_return,period=period)

    @staticmethod
    def alpha_beta(daily_returns, factor_returns,look_back_start = None,look_back_end=None):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.alpha_beta(returns=daily_returns, factor_returns=factor_returns)

    @staticmethod
    def alpha(daily_returns, factor_returns,look_back_start = None,look_back_end=None):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.alpha(returns=daily_returns, factor_returns=factor_returns)

    @staticmethod
    def beta(daily_returns, factor_returns,look_back_start = None,look_back_end=None):
        daily_returns = Analysis_func.cut_returns(daily_returns = daily_returns,look_back_start=look_back_start,look_back_end=look_back_end)
        if daily_returns is None:
            return None
        daily_returns = daily_returns.replace(-numpy.infty, numpy.nan).replace(numpy.infty, numpy.nan).fillna(0)
        return pyfolio.timeseries.beta(returns=daily_returns, factor_returns=factor_returns)

    @staticmethod
    def annual_returns(daily_returns):
        ann_ret_df = pandas.DataFrame(
            ep.aggregate_returns(
                daily_returns,
                'yearly'),columns=['return'])
        return ann_ret_df['return']