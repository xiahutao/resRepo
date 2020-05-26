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

import empyrical as ep
import pyfolio
from pyfolio.utils import DAILY
from analysis.pyfolio_ex.plotting import plot_slippage_sweep,plot_slippage_sensitivity
from config.config import Config_back_test
from analysis.analysis_func import Analysis_func
from settlement.settlement import Settlement

class Analysis(object):

    def __init__(self,
                 strategy_id=None,
                 strategy_type=None,

                 daily_returns=None,
                 daily_return_by_init_aum=None,
                 daily_pnl=None,
                 pnl_daily_dataframe=None,
                 daily_pnl_gross=None,
                 daily_pnl_fee=None,
                 daily_positions=None,
                 transactions=None,
                 round_trips=None,
                 live_start_date=None,
                 result_folder=None,hide_positions=True,
                 init_style=True,
                 **kwargs
                 ):
        """

        :param strategy_id:  指定保存数据库时的id, 确保唯一字符串； 指定为None时不保存
        :param strategy_type: 策略回测结果保存的collection name
        :param daily_returns:
        :param daily_positions:
        :param transactions:
        :param round_trips:
        :param live_start_date:
        :param result_folder:
        :param hide_positions:
        """
        self._strategy_id = strategy_id
        if 'symbols' in kwargs:
            self._symbols = kwargs['symbols']
        else:
            self._symbols = []
        self._strategy_type = strategy_type

        self._live_start_date = live_start_date

        self._daily_returns = daily_returns
        self._daily_return_by_init_aum = daily_return_by_init_aum
        self._daily_pnl = daily_pnl
        self._daily_return_by_init_aum = daily_return_by_init_aum
        self._pnl_daily_dataframe = pnl_daily_dataframe
        self._daily_pnl_gross = daily_pnl_gross
        self._daily_pnl_fee = daily_pnl_fee
        self._daily_positions = daily_positions
        self._transactions = transactions
        self._round_trips = round_trips

        self._init_style = init_style
        self._args={}
        for k in kwargs.keys():
            try:
                if isinstance(kwargs[k], datetime.datetime):
                    self._args[k] =kwargs[k].strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(kwargs[k], pandas.Series):
                    self._args[k] = kwargs[k].to_dict()
                elif isinstance(kwargs[k], set):
                    self._args[k] = list(kwargs[k])
                else:
                    self._args[k] = json.dumps(kwargs[k])
            except:
                pass

        self._positions_dataframe = None
        if self._daily_positions is not None:
            self._positions_dataframe = DataFactory.get_dataframe_pivot(self._daily_positions,
                                                                                   value_column='market_value',
                                                                                   key_column='symbol')
            if self._positions_dataframe is not None and 'cash' not in self._positions_dataframe.columns:
                self._positions_dataframe['cash'] = 0
        self._result_folder = result_folder
        if self._result_folder is not None:
            check_fold(self._result_folder)

    def round_trips(self,onlyclosed=True):
        if self._round_trips is None:
            return None
        if onlyclosed:
            return self._round_trips[self._round_trips['close_dt'].notna()]
        else:
            return self._round_trips

    def load_result(self):
        """
        保存持仓数据，保障重新加载对应文件后，可重现分析结果
        :return:
        """
        if os.path.exists(os.path.join(self._result_folder,'daily_returns.csv')):
            self._daily_returns = pandas.read_csv(os.path.join(self._result_folder,'daily_returns.csv'))
            self._daily_returns.index = pandas.to_datetime(self._daily_returns.index)

        if os.path.exists(os.path.join(self._result_folder,'daily_return_by_init_aum.csv')):
            self._daily_return_by_init_aum = pandas.read_csv(os.path.join(self._result_folder,'daily_return_by_init_aum.csv'))
            self._daily_return_by_init_aum.index = pandas.to_datetime(self._daily_return_by_init_aum.index)

        if os.path.exists(os.path.join(self._result_folder,'pnl_daily_dataframe.csv')):
            self._pnl_daily_dataframe = pandas.read_csv(os.path.join(self._result_folder,'pnl_daily_dataframe.csv'),index_col='trading_date')
            self._pnl_daily_dataframe.index = pandas.to_datetime(self._pnl_daily_dataframe.index)
            if isinstance(self._pnl_daily_dataframe,pandas.DataFrame):
                self._pnl_daily_dataframe = self._pnl_daily_dataframe[self._pnl_daily_dataframe.columns[0]]
        if os.path.exists(os.path.join(self._result_folder,'daily_pnl.csv')):
            self._daily_pnl = pandas.read_csv(os.path.join(self._result_folder,'daily_pnl.csv'),index_col='trading_date')
            self._daily_pnl.index = pandas.to_datetime(self._daily_pnl.index)
            if isinstance(self._daily_pnl,pandas.DataFrame):
                self._daily_pnl = self._daily_pnl[self._daily_pnl.columns[0]]


        if os.path.exists(os.path.join(self._result_folder,'daily_return_by_init_aum.csv')):
            self._daily_return_by_init_aum = pandas.read_csv(os.path.join(self._result_folder,'daily_return_by_init_aum.csv'),index_col='trading_date')
            self._daily_return_by_init_aum.index = pandas.to_datetime(self._daily_pnl.index)
            if isinstance(self._daily_return_by_init_aum,pandas.DataFrame):
                self._daily_return_by_init_aum = self._daily_return_by_init_aum[self._daily_return_by_init_aum.columns[0]]

        if os.path.exists(os.path.join(self._result_folder,'daily_pnl_fee.csv')):
            self._daily_pnl_fee = pandas.read_csv(os.path.join(self._result_folder,'daily_pnl_fee.csv'),index_col='trading_date')
            self._daily_pnl_fee.index = pandas.to_datetime(self._daily_pnl_fee.index)
            if isinstance(self._daily_pnl_fee,pandas.DataFrame):
                self._daily_pnl_fee = self._daily_pnl_fee[self._daily_pnl_fee.columns[0]]
        if os.path.exists(os.path.join(self._result_folder,'daily_pnl_gross.csv')):
            self._daily_pnl_gross = pandas.read_csv(os.path.join(self._result_folder,'daily_pnl_gross.csv'),index_col='trading_date')
            self._daily_pnl_gross.index = pandas.to_datetime(self._daily_pnl_gross.index)
            if isinstance(self._daily_pnl_gross,pandas.DataFrame):
                self._daily_pnl_gross = self._daily_pnl_gross[self._daily_pnl_gross.columns[0]]
        if os.path.exists(os.path.join(self._result_folder,'daily_positions.csv')):
            self._daily_positions = pandas.read_csv(os.path.join(self._result_folder,'daily_positions.csv'),index_col='datetime_index')
            if self._daily_positions is not None and 'market_value' in self._daily_positions.columns:
                self._positions_dataframe = DataFactory.get_dataframe_pivot(self._daily_positions,value_column='market_value',key_column='symbol')
                if 'cash' not in self._positions_dataframe.columns:
                    self._positions_dataframe['cash'] = 0
                    self._positions_dataframe.index = pandas.to_datetime(self._positions_dataframe.index)
        if os.path.exists(os.path.join(self._result_folder,'transactions.csv')):
            self._transactions = pandas.read_csv(os.path.join(self._result_folder,'transactions.csv'))
            if 'DATE_TIME' in self._transactions.columns:
                self._transactions.set_index('DATE_TIME')
                self._transactions.index = pandas.to_datetime(self._transactions.index)
        if os.path.exists(os.path.join(self._result_folder,'round_trips.csv')):
            self._round_trips = pandas.read_csv(os.path.join(self._result_folder,'round_trips.csv'))

    def save_result(self,only_returns=True,**kwargs):
        """
        保存持仓数据，保障重新加载对应文件后，可重现分析结果
        :return:
        """
        fs = file_saver()
        if self._daily_returns is not None:
            fs.save_file(self._daily_returns, os.path.join(self._result_folder, 'daily_returns.csv'))
        if self._daily_return_by_init_aum is not None:
            fs.save_file(self._daily_return_by_init_aum, os.path.join(self._result_folder, 'daily_return_by_init_aum.csv'))
        if only_returns:
            return
        if self._daily_positions is not None:
            # self._daily_positions.to_csv(os.path.join(self._result_folder,'daily_positions.csv'))
            fs.save_file(self._daily_positions, os.path.join(self._result_folder, 'daily_positions.csv'))
            position_panel = self._daily_positions.pivot(columns='symbol', values='position')
            # position_panel.to_csv(os.path.join(self._result_folder,'position_panel.csv'))
            fs.save_file(position_panel, os.path.join(self._result_folder, 'position_panel.csv'))
        if self._daily_pnl is not None:
            fs.save_file(self._daily_pnl, os.path.join(self._result_folder, 'daily_pnl.csv'))
        if self._daily_return_by_init_aum is not None:
            fs.save_file(self._daily_return_by_init_aum, os.path.join(self._result_folder, 'daily_return_by_init_aum.csv'))
        if self._pnl_daily_dataframe is not None:
            fs.save_file(self._pnl_daily_dataframe, os.path.join(self._result_folder, 'pnl_daily_dataframe.csv'))
        if self._daily_pnl_fee is not None:
            fs.save_file(self._daily_pnl_fee, os.path.join(self._result_folder, 'daily_pnl_fee.csv'))
        if self._daily_pnl_gross is not None:
            fs.save_file(self._daily_pnl_gross, os.path.join(self._result_folder, 'daily_pnl_gross.csv'))
        if self._transactions is not None:
            # self._transactions.to_csv(os.path.join(self._result_folder,'transactions.csv'))
            fs.save_file(self._transactions, os.path.join(self._result_folder, 'transactions.csv'))
        if self._round_trips is not None:
            # self._round_trips.to_csv(os.path.join(self._result_folder,'round_trips.csv'))
            fs.save_file(self._round_trips, os.path.join(self._result_folder, 'round_trips.csv'))

    def _save_or_show_plot(self,title,png_file_name=None,show=None,ax=None):
        if title is not None:
            plt.title(title)
        if png_file_name is None:
            png_file_name = title
        if show:
            plt.show()
        elif png_file_name is not None:
            plt.savefig(os.path.join(self._result_folder,png_file_name +'.png'))
        plt.close()

    def _init_ploting_style(self):
        if not self._init_style:
            return
        pyfolio.plotting.plotting_context(font_scale=2)
        fig = plt.figure(figsize=(10, 6))

    def max_drawdown(self):
        return Analysis_func.max_drawdown(daily_returns=self._daily_returns)

    def annual_return(self,period=DAILY):
        return Analysis_func.annual_return(daily_returns=self._daily_returns,period=period)

    def annual_volatility(self,period=DAILY):
        return Analysis_func.annual_volatility(daily_returns=self._daily_returns,period=period)

    def calmar_ratio(self,period=DAILY):
        return Analysis_func.calmar_ratio(daily_returns=self._daily_returns,period=period)

    def omega_ratio(self,annual_return_threshhold=0.0):
        return Analysis_func.omega_ratio(daily_returns=self._daily_returns,annual_return_threshhold=annual_return_threshhold)

    def sortino_ratio(self,period=DAILY):
        return Analysis_func.sortino_ratio(daily_returns=self._daily_returns,period=period)

    def sharpe_ratio(self, risk_free=0,period=DAILY):
        return Analysis_func.sharpe_ratio(daily_returns=self._daily_returns,risk_free=risk_free,period=period)

    def annual_returns(self):
        ann_ret_df = pandas.DataFrame(
            ep.aggregate_returns(
                self._daily_returns,
                'yearly'),columns=['return'])
        return ann_ret_df['return']

    def cum_returns(self, starting_value=0):
        return Analysis_func.cum_returns(daily_returns=self._daily_returns,starting_value=starting_value)

    def downside_risk(self, required_return=0,period=DAILY):
        return Analysis_func.downside_risk(daily_returns=self._daily_returns,required_return=required_return,period=period)

    def alpha_beta(self, factor_returns):
        daily_return  = self._daily_returns
        if daily_return is None:
            return None
        return pyfolio.timeseries.alpha_beta(returns=daily_return, factor_returns=factor_returns)

    def alpha(self, factor_returns):
        daily_return  = self._daily_returns
        if daily_return is None:
            return None
        return pyfolio.timeseries.alpha(returns=daily_return, factor_returns=factor_returns)

    def beta(self, factor_returns):
        daily_return  = self._daily_returns
        if daily_return is None:
            return None
        return pyfolio.timeseries.beta(returns=daily_return, factor_returns=factor_returns)

    def get_top_long_short_abs(self,top=10):
        return pyfolio.pos.get_top_long_short_abs(positions=self._positions_dataframe,top=top)

    def get_max_median_position_concentration(self):
        return pyfolio.pos.get_max_median_position_concentration(positions=self._positions_dataframe)

    def extract_pos(self):
        # todo  positions:columns for amount and last_sale_price. ??
        return pyfolio.pos.extract_pos(positions=self._positions_dataframe,cash=self._positions_dataframe['cash'])

    def get_sector_exposures(self,symbol_sector_map):
        return pyfolio.pos.get_sector_exposures(positions=self._positions_dataframe,symbol_sector_map=symbol_sector_map)

    def get_long_short_pos(self):
        return pyfolio.pos.get_long_short_pos(positions=self._positions_dataframe)

    def create_returns_tear_sheet(self,show=None,ax=None):
        pyfolio.create_returns_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,transactions=self._transactions,ax=ax)
        self._save_or_show_plot(title='create_returns_tear_sheet',show=show)

    def create_position_tear_sheet(self,show=None,ax=None):
        pyfolio.create_position_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,ax=ax)
        self._save_or_show_plot(title='create_position_tear_sheet',show=show)

    def create_txn_tear_sheet(self,show=None,ax=None):
        pyfolio.create_txn_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,transactions=self._transactions,ax=ax)
        self._save_or_show_plot(title='create_txn_tear_sheet',show=show)

    def create_round_trip_tear_sheet(self,show=None,ax=None):
        # ToDo round_trips.add_closing_transactions，   add_closing_transactions运行结果还需要再检查
        pyfolio.create_round_trip_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,transactions=self._transactions,ax=ax)
        self._save_or_show_plot(title='create_round_trip_tear_sheet',show=show)

    def create_interesting_times_tear_sheet(self,show=None,ax=None):
        pyfolio.create_interesting_times_tear_sheet(returns=self._daily_returns)
        self._save_or_show_plot(title='create_interesting_times_tear_sheet',show=show)

    def create_perf_attrib_tear_sheet(self,show=None,ax=None):
        # todo 需要准备 factor_returns,factor_loadings
        pyfolio.create_perf_attrib_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,factor_returns=None,factor_loadings=None,ax=ax)
        self._save_or_show_plot(title='create_perf_attrib_tear_sheet',show=show)

    def create_capacity_tear_sheet(self,show=None,ax=None):
        # todo 需要准备 market_data
        pyfolio.create_capacity_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,transactions=self._transactions,market_data=None,ax=ax)
        self._save_or_show_plot(title='create_capacity_tear_sheet',show=show)

    def create_simple_tear_sheet(self,show=None,ax=None):
        pyfolio.create_simple_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,transactions=self._transactions,ax=ax)
        self._save_or_show_plot(title='create_simple_tear_sheet',show=show)

    def create_full_tear_sheet(self,show=None,ax=None):
        pyfolio.create_full_tear_sheet(returns=self._daily_returns,positions=self._positions_dataframe,transactions=self._transactions)
        self._save_or_show_plot(title='create_full_tear_sheet',show=show)

    def plot_monthly_returns_heatmap(self,show=None,ax=None):
        if ax is not None:
            ax.set_title('plot_monthly_returns_heatmap')
        pyfolio.plotting.plot_monthly_returns_heatmap(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_monthly_returns_heatmap',show=show)

    def plot_monthly_returns_dist(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_monthly_returns_dist')
        pyfolio.plotting.plot_monthly_returns_dist(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_monthly_returns_dist',show=show)

    def plot_holdings(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_holdings')
        pyfolio.plotting.plot_holdings(returns=self._daily_returns,positions=self._positions_dataframe,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_holdings',show=show)

    def plot_long_short_holdings(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_long_short_holdings')
        pyfolio.plotting.plot_long_short_holdings(returns=self._daily_returns,positions=self._positions_dataframe,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_long_short_holdings',show=show)

    def plot_drawdown_periods(self, top=10,show=None,ax=None):
        self._init_ploting_style()
        pyfolio.plotting.plot_drawdown_periods(returns=self._daily_returns, top=top,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_drawdown_periods',show=show)

    def plot_drawdown_underwater(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_drawdown_underwater')
        pyfolio.plotting.plot_drawdown_underwater(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_drawdown_underwater',show=show)

    def show_perf_stats(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('show_perf_stats')
        pyfolio.plotting.show_perf_stats(returns=self._daily_returns)
        if show is not None:
            self._save_or_show_plot(title='show_perf_stats',show=show)

    def plot_returns(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_returns')
        pyfolio.plotting.plot_returns(returns=self._daily_returns,ax=ax)
        self._save_or_show_plot(title='plot_returns',show=show)

    def plot_annual_returns(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_annual_returns')
        pyfolio.plotting.plot_annual_returns(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_annual_returns',show=show)

    def plot_cumsum_pnl(self,show=None,ax=None,title=None):
        if self._daily_pnl is None:
            return
        self._init_ploting_style()
        if ax is None:
            ax = plt.gca()
        if title is not None:
            ax.set_title(title)
        self._daily_pnl.cumsum().plot(ax=ax,alpha=0.70)
        self._daily_pnl_gross.cumsum().plot(ax=ax,alpha=0.70)
        self._daily_pnl_fee.cumsum().plot(ax=ax,alpha=0.70)
        # ax.legend(loc='upper left')

        if show is not None:
            self._save_or_show_plot(title=None,png_file_name='plot_cumsum_pnl',show=show)


    def get_index_dict(self, format=False):
        sharpe_ratio = self.sharpe_ratio()
        sortino_ratio = self.sortino_ratio()
        calmar_ratio = self.calmar_ratio()
        max_drawdown = self.max_drawdown()
        annual_return = self.annual_return()
        annual_volatility = self.annual_volatility()
        if not format:
            return {'sharpe':sharpe_ratio,'annual_ret':annual_return,'annual_vol':annual_volatility
                ,'max_dd':max_drawdown,'sortino':sortino_ratio,'calmar':calmar_ratio
                    }
        else:
            return {'sharpe': '%.3f' % sharpe_ratio,'annual_ret':'%.3f%%' % (annual_return*100),'annual_vol':'%.3f%%' % (annual_volatility*100)
                ,'max_dd':'%.3f%%' % (max_drawdown*100),'sortino':'%.3f' % sortino_ratio,'calmar':'%.3f' % calmar_ratio
                    }

    def plot_cumulative_returns(self,show=None,ax=None):
        self._init_ploting_style()
        pyfolio.plotting.plot_rolling_returns(returns=self._daily_returns,live_start_date=self._live_start_date,ax=ax,factor_returns=None)
        # sharpe_ratio = self.sharpe_ratio()
        # sortino_ratio = self.sortino_ratio()
        # calmar_ratio = self.calmar_ratio()
        # max_drawdown = self.max_drawdown()
        #
        # title_str = 'sharpe %.2f sortino %.2f calmar %.2f max_drawdown %.2f' % (sharpe_ratio,sortino_ratio,calmar_ratio,max_drawdown)
        # if ax is not None:
        #     ax.set_title(title_str)
        # else:
        #     plt.title(title_str)
        if show is not None:
            self._save_or_show_plot(title=None,png_file_name='plot_rolling_returns',show=show)


    def plot_rolling_returns(self,show=None,ax=None):
        self._init_ploting_style()
        pyfolio.plotting.plot_rolling_returns(returns=self._daily_returns,live_start_date=self._live_start_date,ax=ax,factor_returns=None)
        sharpe_ratio = self.sharpe_ratio()
        sortino_ratio = self.annual_return()
        calmar_ratio = self.calmar_ratio()
        max_drawdown = self.max_drawdown()

        title_str = 'sharpe %.2f ann_ret %.2f calmar %.2f max_drawdown %.2f' % (sharpe_ratio,sortino_ratio,calmar_ratio,max_drawdown)
        if ax is not None:
            ax.set_title(title_str)
        else:
            plt.title(title_str)
        if show is not None:
            self._save_or_show_plot(title=None,png_file_name='plot_rolling_returns',show=show)

    def plot_rolling_volatility(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_rolling_volatility')
        pyfolio.plotting.plot_rolling_volatility(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_rolling_volatility',show=show)

    def plot_exposures(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_exposures')
        pyfolio.plotting.plot_exposures(returns=self._daily_returns,positions=self._positions_dataframe,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_exposures',show=show)

    def plot_max_median_position_concentration(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_max_median_position_concentration')
        pyfolio.plotting.plot_max_median_position_concentration(positions=self._positions_dataframe,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_max_median_position_concentration',show=show)

    def plot_sector_allocations(self,show=None,ax=None):
        # ToDo: plot_sector_allocations
        pass

    def plot_gross_leverage(self,show=None,ax=None):
        # ToDo: plot_gross_leverage
        # sum(头寸）/ sum(头寸&现金），需要确定持仓剩余现金。  另， 期货杠杆定义不同。
        # 待实现
        # pyfolio.plotting.plot_gross_leverage()
        # self._save_or_show_plot(title='plot_gross_leverage',show=show)
        pass

    def plot_slippage_sweep(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_slippage_sweep')
        returns = self._daily_returns.copy()
        positions = self._positions_dataframe.copy()
        transactions = self._transactions.copy()

        returns.index = returns.index.normalize()
        positions.index = positions.index.normalize()
        transactions.set_index('trading_date',inplace=True)
        transactions.index = (pandas.to_datetime (pandas.DatetimeIndex(transactions.index).date) + datetime.timedelta(hours=15)).tz_localize(setting.DEFAULT_TIMEZONE) #pandas.to_datetime(transactions.index + ' 15:00:00').tz_localize(setting.DEFAULT_TIMEZONE)

        # positions = numpy.abs(positions)

        plot_slippage_sweep(returns=returns,positions=positions,transactions=transactions,ax=ax
                            ,slippage_params=(0,3, 8, 10, 12, 15, 20, 50))
        if show is not None:
            self._save_or_show_plot(title='plot_slippage_sweep',show=show)

    def plot_slippage_sensitivity(self,show=None,ax=None):
        #todo, adjust_returns_for_slippage 没有考虑做空部分，导致结果不合理
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_slippage_sensitivity')
        returns = self._daily_returns.copy()
        positions = self._positions_dataframe.copy()
        transactions = self._transactions.copy()

        returns.index = returns.index.normalize()
        positions.index = positions.index.normalize()
        transactions.set_index('trading_date',inplace=True)
        transactions.index = (pandas.to_datetime (pandas.DatetimeIndex(transactions.index).date) + datetime.timedelta(hours=15)).tz_localize(setting.DEFAULT_TIMEZONE) #pandas.to_datetime(transactions.index + ' 15:00:00').tz_localize(setting.DEFAULT_TIMEZONE)
        plot_slippage_sensitivity(returns=returns,positions=positions,transactions=transactions,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_slippage_sensitivity',show=show)

    def plot_daily_turnover_hist(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_daily_turnover_hist')
        transactions = self._transactions.copy()
        transactions.set_index('trading_date',inplace=True)
        transactions.index = (pandas.to_datetime (pandas.DatetimeIndex(transactions.index).date) + datetime.timedelta(hours=15)).tz_localize(setting.DEFAULT_TIMEZONE) #pandas.to_datetime(transactions.index + ' 15:00:00').tz_localize(setting.DEFAULT_TIMEZONE)
        pyfolio.plotting.plot_daily_turnover_hist(positions=self._positions_dataframe,transactions=transactions,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_daily_turnover_hist',show=show)

    def plot_daily_volume(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_daily_volume')
        pyfolio.plotting.plot_daily_volume(returns=self._daily_returns,transactions=self._transactions,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_daily_volume',show=show)

    def plot_txn_time_hist(self,bin_minutes=5,tz='Asia/Shanghai',show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_txn_time_hist')
        pyfolio.plotting.plot_txn_time_hist(transactions=self._transactions,bin_minutes=bin_minutes,tz=tz,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_txn_time_hist',show=show)

    def show_worst_drawdown_periods(self,top=5,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('show_worst_drawdown_periods')
        pyfolio.plotting.show_worst_drawdown_periods(returns=self._daily_returns,top=top)
        if show is not None:
            self._save_or_show_plot(title='show_worst_drawdown_periods',show=show)

    def plot_monthly_returns_timeseries(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_monthly_returns_timeseries')
        pyfolio.plotting.plot_monthly_returns_timeseries(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_monthly_returns_timeseries',show=show)

    def plot_round_trip_lifetimes(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_round_trip_lifetimes')
        pyfolio.plotting.plot_round_trip_lifetimes(round_trips=self.round_trips(),ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_round_trip_lifetimes',show=show)

    def show_profit_attribution(self,show=None,ax=None):
        if ax is not None:
            ax.set_title('Profitability (PnL / PnL total) per name')
        # pyfolio.plotting.show_profit_attribution(round_trips=self.round_trips())
        round_trips = self.round_trips()

        total_pnl = round_trips['pnl'].sum()
        pnl_attribution = round_trips.groupby('symbol')['pnl'].sum() / total_pnl
        pnl_attribution.name = 'pnl_attribution'

        pnl_attribution.index = pnl_attribution.index.map(pyfolio.utils.format_asset)
        pnl_attribution_table = pandas.DataFrame(pnl_attribution)
        if ax is not None:
            ax.table(cellText=pnl_attribution_table, colWidths=[0.1] * 3, loc='best')
        else:
            plt.table(cellText=numpy.array(pnl_attribution_table),rowLabels=pnl_attribution_table.index,colLabels=pnl_attribution_table.columns, colWidths=[0.1] * 3, loc='best')
        #
        # pyfolio.utils.print_table(
        #     pnl_attribution.sort_values(
        #         inplace=False,
        #         ascending=False,
        #     ),
        #     name='Profitability (PnL / PnL total) per name',
        #     float_format='{:.2%}'.format,
        # )

        if show is not None:
            self._save_or_show_plot(title='show_profit_attribution',show=show)

    def plot_prob_profit_trade(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_prob_profit_trade')
        pyfolio.plotting.plot_prob_profit_trade(round_trips=self.round_trips(),ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_prob_profit_trade',show=show)

    def plot_turnover(self,show=None,ax=None):
        self._init_ploting_style()
        transactions = self._transactions.copy()
        transactions.set_index('trading_date',inplace=True)
        transactions.index = (pandas.to_datetime (pandas.DatetimeIndex(transactions.index).date) + datetime.timedelta(hours=15)).tz_localize(setting.DEFAULT_TIMEZONE) #pandas.to_datetime(transactions.index + ' 15:00:00').tz_localize(setting.DEFAULT_TIMEZONE)
        positions = self._positions_dataframe
        pyfolio.plotting.plot_turnover(returns=self._daily_returns,transactions=transactions,positions=positions,ax=ax)
        self._save_or_show_plot(title='plot_turnover',show=show)
        pass

    def plot_return_quantiles(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_return_quantiles')
        pyfolio.plotting.plot_return_quantiles(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_return_quantiles',show=show)

    def plot_rolling_sharpe(self,show=None,ax=None):
        self._init_ploting_style()
        if ax is not None:
            ax.set_title('plot_rolling_sharpe')
        pyfolio.plotting.plot_rolling_sharpe(returns=self._daily_returns,ax=ax)
        if show is not None:
            self._save_or_show_plot(title='plot_rolling_sharpe',show=show)

    # def plot_drawdown_periods(self,show=None,ax=None):
    #     pyfolio.plotting.plot_drawdown_periods(returns=self._daily_returns)
    #     if show:
    #         plt.show()

    def summary(self):
        retdict = {}
        retdict['sharpe_ratio'] = self.sharpe_ratio()
        retdict['sortino_ratio'] = self.sortino_ratio()
        retdict['max_dd'] = self.max_drawdown()
        retdict['cum_returns'] = self.cum_returns()
        return retdict

    def report_return(self,title='return'):
        return
        fig = plt.figure(figsize=(9, 9),facecolor='gray')

        sharpe_ratio = self.sharpe_ratio()
        sortino_ratio = self.sortino_ratio()
        calmar_ratio = self.calmar_ratio()

        picrow, piccol, pic = 3,1,1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_rolling_returns(ax=ax)
        ax.set_title('sharpe %.2f sortino %.2f  calmar %.2f' % (sharpe_ratio,sortino_ratio,calmar_ratio))

        picrow, piccol, pic = 3,3,4
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_drawdown_periods(ax=ax)

        picrow, piccol, pic = 3,3,5
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_monthly_returns_timeseries(ax=ax)

        picrow, piccol, pic = 3,3,6
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_annual_returns(ax=ax)

        picrow, piccol, pic = 3,3,7
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_returns(ax=ax)

        picrow, piccol, pic = 3,3,8
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_txn_time_hist(ax=ax)

        picrow, piccol, pic = 3,3,9
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_return_quantiles(ax=ax)

        plt.savefig(os.path.join(self._result_folder, title + '.png'))
        plt.close()

    def report(self,title):
        fig = plt.figure(figsize=(16, 9))
        picrow, piccol, pic = 3,3,1

        ax = plt.subplot(picrow, piccol, pic)
        self.plot_annual_returns(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_rolling_returns(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_monthly_returns_heatmap(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_holdings(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_drawdown_underwater(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_monthly_returns_timeseries(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_prob_profit_trade(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_txn_time_hist(ax=ax)

        pic +=1
        ax = plt.subplot(picrow, piccol, pic)
        self.plot_exposures(ax=ax)

        plt.savefig(os.path.join(self._result_folder, title + '.png'))
        plt.close()

    def plot_all(self,show=False):
        for method in self.methods():
            if 'plot_' not in method:
                continue
            if method in ['plot_all','plot_round_trip_lifetimes']:
                continue
            try:
                print(method)
                getattr(self,method)(show=show)
            except:
                print(('error',method))
                traceback.print_exc()

    def methods(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))

class Analysis_ex(Analysis):
    def __init__(self,config,settlement_obj,hide_positions=True,
                 init_style=True,
                 **kwargs):
        self._config = config
        assert isinstance(config,Config_back_test)
        assert isinstance(settlement_obj,Settlement)
        self._settlement_obj = settlement_obj
        strategy_config = copy.copy(config.strategy_config)
        if 'strategy_type' in strategy_config:
            strategy_config.pop('strategy_type')
        Analysis.__init__(self,
                 strategy_id=config.strategy_id,
                 strategy_type=config.strategy_type,
                          daily_return_by_init_aum = settlement_obj.daily_return_by_init_aum,
                 daily_returns=settlement_obj.daily_return,
                 daily_pnl=settlement_obj.daily_pnl,
                 daily_pnl_gross=settlement_obj.daily_pnl_gross,
                 daily_pnl_fee=settlement_obj.daily_pnl_fee,
                 daily_positions=settlement_obj.daily_positions,
                 transactions=settlement_obj.transactions,
                 round_trips=settlement_obj.round_trips,
                 live_start_date=config.get_strategy_config('live_start_date'),
                 result_folder=config.get_result_config('result_folder'),hide_positions=hide_positions,
                 init_style=init_style,
                          **strategy_config)

        self._mongo_client = DataFactory.get_mongo_client()
        self._strategy_log_db = self._mongo_client.get_database('strategy_log')

    def __del__(self):
        self._mongo_client.close()

    @property
    def settlement_obj(self):
        return self._settlement_obj

    @runing_time
    def save_result_stragety_log(self,collection_name, series_tmp,from_date=None,to_date = None):
        if series_tmp is not None:
            cl = self._strategy_log_db.get_collection(collection_name)
            cl.ensure_index(key_or_list=[('strategy_id', -1), ('date_index', 1)])
            cl.ensure_index(key_or_list='strategy_type')
            filter = {'strategy_id': self._strategy_id}
            if from_date is not None and to_date is not None:
                filter = {'$and': [filter, {'date_index': {'$gte': from_date}}, {'date_index': {'$lte': to_date}}]}
            elif to_date is not None:
                filter = {'$and': [filter, {'date_index': {'$lte': to_date}}]}
            elif from_date is not None:
                filter = {'$and': [filter, {'date_index': {'$gte': from_date}}]}
            cl.delete_many(filter=filter)

            if isinstance(series_tmp, pandas.Series):
                if len(series_tmp)>0:
                    if isinstance(series_tmp[0],datetime.date):
                        series_tmp = pandas.to_datetime(series_tmp)
                    if from_date is not None and to_date is not None:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, collection_name: y} for x, y in series_tmp.items() if x >= from_date and x <= to_date]
                    elif to_date is not None:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, collection_name: y} for x, y in series_tmp.items() if x <= to_date]
                    elif from_date is not None:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, collection_name: y} for x, y in series_tmp.items() if x >= from_date]
                    else:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, collection_name: y} for x, y in series_tmp.items()]
                    cl.insert_many(daily_returns_list)
            elif isinstance(series_tmp, pandas.DataFrame):
                if len(series_tmp)>0:
                    for col in series_tmp.columns:
                        if isinstance(series_tmp.iloc[0][col],datetime.date):
                            series_tmp[col] = pandas.to_datetime(series_tmp[col])
                    if from_date is not None and to_date is not None:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, **y} for x, y in series_tmp.iterrows() if x >= from_date and x <= to_date]
                    elif to_date is not None:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, **y} for x, y in series_tmp.iterrows() if x <= to_date]
                    elif from_date is not None:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, **y} for x, y in series_tmp.iterrows() if x >= from_date]
                    else:
                        daily_returns_list = [
                            {'strategy_id': self._strategy_id, 'strategy_type': self._config.strategy_type, 'date_index': x, **y} for x, y in series_tmp.iterrows()]
                cl.insert_many(daily_returns_list)

    @runing_time
    def save_result(self,from_date = None, to_date = None,only_returns=True, **kwargs):
        Analysis.save_result(self,only_returns=only_returns,**kwargs)
        if only_returns:
            return
        if self._strategy_id is not None:
            if self._strategy_id is not None:
                _mongo_client = DataFactory.get_mongo_client()
                _strategy_log_db = _mongo_client.get_database('strategy_log')
                db = _strategy_log_db

                cl = db.get_collection('strategy_info')
                cl.ensure_index(key_or_list=[('strategy_id', -1)])
                sharpe_ratio = self.sharpe_ratio()
                sortino_ratio = self.sortino_ratio()
                calmar_ratio = self.calmar_ratio()
                max_drawdown = self.max_drawdown()
                cum_returns = self.cum_returns(starting_value=1)
                meta_data = {}
                meta_data['strategy_type'] = self._config.strategy_type
                if self._symbols is not None:
                    meta_data['symbols'] = '_'.join(self._symbols)
                meta_data['sharpe_ratio'] = sharpe_ratio
                meta_data['sharpe_ratio'] = sharpe_ratio
                meta_data['sortino_ratio'] = sortino_ratio
                meta_data['calmar_ratio'] = calmar_ratio
                meta_data['max_dd'] = max_drawdown
                meta_data['config']=repr(self._config)
                if cum_returns is not None and len(cum_returns) > 0:
                    meta_data['cum_returns'] = cum_returns.iloc[-1]
                for x, y in self._args.items():
                    meta_data[x] = y
                meta_data['update_time'] = datetime.datetime.now()
                cl.update_one(filter={'strategy_id': self._strategy_id}
                              , update={'$set': meta_data}, upsert=True)
                _mongo_client.close()
            for nm, series_tmp in {'daily_returns':self._daily_returns,
                                   'daily_return_by_init_aum':self._settlement_obj.daily_return_by_init_aum,
                                   'daily_pnl':self._daily_pnl,
                                   'daily_return_by_init_aum':self._daily_return_by_init_aum,
                                   'daily_pnl_gross':self._daily_pnl_gross,
                                   'daily_pnl_fee':self._daily_pnl_fee
                                   }.items():
                self.save_result_stragety_log(collection_name=nm,series_tmp=series_tmp,from_date=from_date,to_date=to_date)