# -*- coding: utf-8 -*-

import pandas
import numpy
import time
import datetime
import os
import dask.dataframe as ddf
from multiprocessing import cpu_count
import pyfolio
from functools import reduce,partial
from collections import deque, OrderedDict
import warnings
from math import copysign

from common.decorator import runing_time
from data_engine.data_factory import DataFactory
from data_engine.setting import DATASOURCE_LOCAL,DATASOURCE_REMOTE
from data_engine.setting import FREQ_1M,FREQ_1D,FREQ_TICK,FREQ_5M,DEFAULT_TIMEZONE,PRICE_TYPE_UN

from settlement.fee.fee_ghls import Fee_ghls
from settlement.fee.fee import Fee
from settlement.fee.fee_costs import Fee_costs as fee_costs_file



class Settlement(object):
    """
    根据pyfolio的接口，需要对撮合完成后的持仓进行清算，得到每日持仓，和组合后的return;
    增加清算模块
    """
    def __init__(self,init_aum=1000000,fee_func = None,price_type=PRICE_TYPE_UN):
        self._positions_dataframe=None

        self._transactions_dataframe=None
        self._round_trips=None
        self._positions_daily_dataframe=None

        if fee_func is None:
            self._fee_func= Fee_ghls()
        else:
            self._fee_func= Fee()
        self._init_aum = init_aum
        self._price_type=price_type

    def _load_transactions(self,result_folder='e://resRepo_tmp'):
        self._positions_dataframe = pandas.DataFrame.from_csv(os.path.join(result_folder, 'positions_dataframe.csv'))

    def _save_transactions(self,positions_dataframe,result_folder='e://resRepo_tmp'):
        if positions_dataframe.index.tzinfo is not None:
            positions_dataframe.index = positions_dataframe.index.tz_convert(DEFAULT_TIMEZONE)
        positions_dataframe.to_csv(os.path.join(result_folder, 'positions_dataframe.csv'))

    @staticmethod
    def _map_row(x):
        return pandas.concat([x] * int(x['abs_amount']), axis=1)

    @staticmethod
    @runing_time
    def extract_round_trips_0(transactions,
                            portfolio_value=None):
        """Group transactions into "round trips". First, transactions are
        grouped by day and directionality. Then, long and short
        transactions are matched to create round-trip round_trips for which
        PnL, duration and returns are computed. Crossings where a position
        changes from long to short and vice-versa are handled correctly.

        Under the hood, we reconstruct the individual shares in a
        portfolio over time and match round_trips in a FIFO-order.

        For example, the following transactions would constitute one round trip:
        index                  amount   price    symbol
        2004-01-09 12:18:01    10       50      'AAPL'
        2004-01-09 15:12:53    10       100      'AAPL'
        2004-01-13 14:41:23    -10      100      'AAPL'
        2004-01-13 15:23:34    -10      200       'AAPL'

        First, the first two and last two round_trips will be merged into a two
        single transactions (computing the price via vwap). Then, during
        the portfolio reconstruction, the two resulting transactions will
        be merged and result in 1 round-trip trade with a PnL of
        (150 * 20) - (75 * 20) = 1500.

        Note, that round trips do not have to close out positions
        completely. For example, we could have removed the last
        transaction in the example above and still generated a round-trip
        over 10 shares with 10 shares left in the portfolio to be matched
        with a later transaction.

        Parameters
        ----------
        transactions : pd.DataFrame
            Prices and amounts of executed round_trips. One row per trade.
            - See full explanation in tears.create_full_tear_sheet

        portfolio_value : pd.Series (optional)
            Portfolio value (all net assets including cash) over time.
            Note that portfolio_value needs to beginning of day, so either
            use .shift() or positions.sum(axis='columns') / (1+returns).

        Returns
        -------
        round_trips : pd.DataFrame
            DataFrame with one row per round trip.  The returns column
            contains returns in respect to the portfolio value while
            rt_returns are the returns in regards to the invested capital
            into that partiulcar round-trip.
        """

        # transactions = pyfolio.round_trips._groupby_consecutive(transactions)

        # pool = Pool(max(1,cpu_count() - 1))
        roundtrips = []
        transactions_tmp = transactions.copy()

        transactions_tmp['signed_price'] = transactions_tmp.price * \
                                    numpy.sign(transactions_tmp.amount)
        transactions_tmp['abs_amount'] = transactions_tmp.amount.abs().astype(int)
        transactions_tmp.index.name= 'date_index'
        transactions_tmp = transactions_tmp.sort_index()
        transactions_tmp.reset_index(inplace=True)

        for sym, trans_sym in transactions_tmp.groupby('symbol'):
            trans_sym_p = trans_sym[trans_sym['signed_price'] > 0]
            trans_sym_n = trans_sym[trans_sym['signed_price'] < 0]

            rows_list_p = [x for id_tmp ,x in trans_sym_p[['abs_amount','date_index','signed_price']].iterrows()]
            # rows_list_p = pool.map(Settlement._map_row,rows_list_p)
            # pool.close()
            # pool.join()
            rows_list_p = list(map(Settlement._map_row, rows_list_p))
            trans_sym_p_bigtable = pandas.concat(rows_list_p)
            trans_sym_p_bigtable['abs_amount'] = 1
            trans_sym_p_bigtable.sort_values('date_index',inplace=True)
            trans_sym_p_bigtable['abs_amount_cumsum'] = trans_sym_p_bigtable['abs_amount'] .cumsum()
            trans_sym_p_bigtable.set_index('abs_amount_cumsum',inplace=True)

            rows_list_n = [x for id_tmp ,x in trans_sym_n[['abs_amount','date_index','signed_price']].iterrows()]
            # rows_list_n = pool.map(Settlement._map_row,rows_list_n)
            # pool.close()
            # pool.join()
            rows_list_n = list(map(Settlement._map_row, rows_list_n))
            trans_sym_n_bigtable = pandas.concat(rows_list_n)
            trans_sym_n_bigtable['abs_amount'] = 1
            trans_sym_n_bigtable.sort_values('date_index',inplace=True)
            trans_sym_n_bigtable['abs_amount_cumsum'] = trans_sym_n_bigtable['abs_amount'] .cumsum()
            trans_sym_n_bigtable.set_index('abs_amount_cumsum',inplace=True)

            trans_sym_bigtable = pandas.merge(trans_sym_p_bigtable,trans_sym_n_bigtable,how='outer',left_index=True,right_index=True,suffixes=('_p','_n'))
            # trans_sym_bigtable_sum_groupby = trans_sym_bigtable.groupby(['date_index_p','signed_price_p','date_index_n','signed_price_n']).sum()

            for dt_p,signed_price_p,dt_n,signed_price_n, sub_dataframe  in trans_sym_bigtable.groupby(['date_index_p','signed_price_p','date_index_n','signed_price_n']):
                abs_amount = len(sub_dataframe)
                if dt_p < dt_n:
                    open_price = signed_price_p
                    close_price = signed_price_n
                    open_dt = dt_p
                    close_dt = dt_n
                else:
                    open_price = signed_price_n
                    close_price = signed_price_p
                    open_dt = dt_n
                    close_dt = dt_p
                pnl = -(open_price + close_price) * abs_amount
                roundtrips.append({'pnl': pnl,
                                   'abs_amount': abs_amount,
                                   'open_price': numpy.mean(open_price),
                                   'close_price': numpy.mean(close_price),
                                   'open_dt': open_dt,
                                   'close_dt': close_dt,
                                   'long': open_price < 0,
                                   'rt_returns': numpy.abs(close_price / open_price) - 1,
                                   'symbol': sym,
                                   })

        roundtrips = pandas.DataFrame(roundtrips)
        roundtrips_tmp = roundtrips[roundtrips['close_dt'].notna()]
        if not roundtrips_tmp.empty:
            roundtrips_tmp.loc[:,'duration'] = roundtrips_tmp['close_dt'].sub(roundtrips_tmp['open_dt'])

        if portfolio_value is not None:
            # Need to normalize so that we can join
            pv = pandas.DataFrame(portfolio_value,
                              columns=['portfolio_value']) \
                .assign(date=portfolio_value.index)

            roundtrips['date'] = roundtrips.close_dt.apply(lambda x:
                                                           x.replace(hour=0,
                                                                     minute=0,
                                                                     second=0))

            tmp = roundtrips.join(pv, on='date', lsuffix='_')

            roundtrips['returns'] = tmp.pnl / tmp.portfolio_value
            roundtrips = roundtrips.drop('date', axis='columns')

        roundtrips['open_dt'] = pandas.to_datetime(roundtrips['open_dt'])
        roundtrips['close_dt'] = pandas.to_datetime(roundtrips['close_dt'])
        return roundtrips

    @staticmethod
    @runing_time
    def extract_round_trips(transactions,
                            portfolio_value=None):
        """Group transactions into "round trips". First, transactions are
        grouped by day and directionality. Then, long and short
        transactions are matched to create round-trip round_trips for which
        PnL, duration and returns are computed. Crossings where a position
        changes from long to short and vice-versa are handled correctly.

        Under the hood, we reconstruct the individual shares in a
        portfolio over time and match round_trips in a FIFO-order.

        For example, the following transactions would constitute one round trip:
        index                  amount   price    symbol
        2004-01-09 12:18:01    10       50      'AAPL'
        2004-01-09 15:12:53    10       100      'AAPL'
        2004-01-13 14:41:23    -10      100      'AAPL'
        2004-01-13 15:23:34    -10      200       'AAPL'

        First, the first two and last two round_trips will be merged into a two
        single transactions (computing the price via vwap). Then, during
        the portfolio reconstruction, the two resulting transactions will
        be merged and result in 1 round-trip trade with a PnL of
        (150 * 20) - (75 * 20) = 1500.

        Note, that round trips do not have to close out positions
        completely. For example, we could have removed the last
        transaction in the example above and still generated a round-trip
        over 10 shares with 10 shares left in the portfolio to be matched
        with a later transaction.

        Parameters
        ----------
        transactions : pd.DataFrame
            Prices and amounts of executed round_trips. One row per trade.
            - See full explanation in tears.create_full_tear_sheet

        portfolio_value : pd.Series (optional)
            Portfolio value (all net assets including cash) over time.
            Note that portfolio_value needs to beginning of day, so either
            use .shift() or positions.sum(axis='columns') / (1+returns).

        Returns
        -------
        round_trips : pd.DataFrame
            DataFrame with one row per round trip.  The returns column
            contains returns in respect to the portfolio value while
            rt_returns are the returns in regards to the invested capital
            into that partiulcar round-trip.
        """

        # transactions = pyfolio.round_trips._groupby_consecutive(transactions)
        roundtrips = []
        transactions_tmp = transactions.copy()
        for sym, trans_sym in transactions_tmp.groupby('symbol'):
            trans_sym = trans_sym.sort_index()
            price_stack = deque()
            dt_stack = deque()
            trans_sym['signed_price'] = trans_sym.price * \
                                        numpy.sign(trans_sym.amount)
            trans_sym['abs_amount'] = trans_sym.amount.abs().astype(int)
            for dt, t in trans_sym.iterrows():
                if t.price < 0:
                    warnings.warn('Negative price detected, ignoring for'
                                  'round-trip.')
                    continue
                if t.abs_amount == 0 or numpy.isnan(t.price):
                    print((dt,t,'extract_round_trips t.abs_amount == 0 or numpy.isnan(t.price)'))
                    continue
                #todo, 由于其他地方bug，可能导致t.abs_amount=0， 这里会运行错误。 暂时不处理，方便发现其他环节错误，比如非交易时间有量价数据等
                indiv_prices = [t.signed_price] * t.abs_amount
                if (len(price_stack) == 0) or \
                        (copysign(1, price_stack[-1]) == copysign(1, t.amount)):
                    price_stack.extend(indiv_prices)
                    dt_stack.extend([dt] * len(indiv_prices))
                else:
                    # Close round-trip
                    pnl = 0
                    open_price=[]
                    close_price=[]
                    abs_amount=0
                    invested = 0
                    cur_open_dts = []

                    for price in indiv_prices:
                        if len(price_stack) != 0 and \
                                (copysign(1, price_stack[-1]) != copysign(1, price)):
                            # Retrieve first dt, stock-price pair from
                            # stack
                            prev_price = price_stack.popleft()
                            prev_dt = dt_stack.popleft()

                            if len(cur_open_dts)>0 and prev_dt != cur_open_dts[0]:
                                roundtrips.append({'pnl': pnl,
                                                   'abs_amount': abs_amount,
                                                   'open_price': numpy.mean(open_price),
                                                   'close_price': numpy.mean(close_price),
                                                   'open_dt': cur_open_dts[0],
                                                   'close_dt': dt,
                                                   'long': price < 0,
                                                   'rt_returns': pnl / invested,
                                                   'symbol': sym,
                                                   })
                                pnl = 0
                                open_price = []
                                close_price = []
                                abs_amount = 0
                                invested = 0
                                cur_open_dts = []

                            open_price.append(prev_price)
                            close_price.append(price)
                            pnl += -(price + prev_price)
                            cur_open_dts.append(prev_dt)
                            invested += abs(prev_price)
                            abs_amount+=1

                        else:
                            # Push additional stock-prices onto stack
                            price_stack.append(price)
                            dt_stack.append(dt)
                    # print('extract_round_trips',sym,dt,t)
                    if len(cur_open_dts) == 0:
                        print(dt,t,'len(cur_open_dts) == 0')
                    else:
                        roundtrips.append({'pnl': pnl,
                                           'abs_amount':abs_amount,
                                           'open_price':numpy.mean(open_price),
                                           'close_price':numpy.mean(close_price),
                                           'open_dt': cur_open_dts[0],
                                           'close_dt': dt,
                                           'long': price < 0,
                                           'rt_returns': pnl / invested,
                                           'symbol': sym,
                                           })

            while len(price_stack) >0:
                pnl = 0
                open_price = []
                close_price = []
                abs_amount = 0
                invested = 0
                cur_open_dts = []
                while len(price_stack)>0:
                    prev_price = price_stack.popleft()
                    prev_dt = dt_stack.popleft()
                    if len(cur_open_dts) > 0 and prev_dt != cur_open_dts[0]:
                        roundtrips.append({'pnl': numpy.nan,
                                           'abs_amount': abs_amount,
                                           'open_price': numpy.mean(open_price),
                                           'close_price': numpy.nan,
                                           'open_dt': cur_open_dts[0],
                                           'close_dt': numpy.nan,
                                           'long': prev_price > 0,
                                           'rt_returns': numpy.nan,
                                           'symbol': sym,
                                           })
                        pnl = 0
                        open_price = []
                        close_price = []
                        abs_amount = 0
                        invested = 0
                        cur_open_dts = []

                    open_price.append(prev_price)
                    cur_open_dts.append(prev_dt)
                    abs_amount += 1
                if len(cur_open_dts)>0:
                    roundtrips.append({'pnl': numpy.nan,
                                       'abs_amount': abs_amount,
                                       'open_price': numpy.mean(open_price),
                                       'close_price': numpy.nan,
                                       'open_dt': cur_open_dts[0],
                                       'close_dt': numpy.nan,
                                       'long': prev_price > 0,
                                       'rt_returns': numpy.nan,
                                       'symbol': sym,
                                       })

        roundtrips = pandas.DataFrame(roundtrips)
        roundtrips_tmp = roundtrips[roundtrips['close_dt'].notna()]
        if not roundtrips_tmp.empty:
            roundtrips_tmp.loc[:,'duration'] = roundtrips_tmp['close_dt'].sub(roundtrips_tmp['open_dt'])

        if portfolio_value is not None:
            # Need to normalize so that we can join
            pv = pandas.DataFrame(portfolio_value,
                              columns=['portfolio_value']) \
                .assign(date=portfolio_value.index)

            roundtrips['date'] = roundtrips.close_dt.apply(lambda x:
                                                           x.replace(hour=0,
                                                                     minute=0,
                                                                     second=0))

            tmp = roundtrips.join(pv, on='date', lsuffix='_')

            roundtrips['returns'] = tmp.pnl / tmp.portfolio_value
            roundtrips = roundtrips.drop('date', axis='columns')

        roundtrips['open_dt'] = pandas.to_datetime(roundtrips['open_dt'])
        roundtrips['close_dt'] = pandas.to_datetime(roundtrips['close_dt'])
        return roundtrips

    # def settle_v0(self,positions_dataframe):
    #     #_round_trips 逐日计算收益方法
    #     self._positions_dataframe = positions_dataframe
    #
    #     #确定交易流水 transactions
    #     self._transactions_dataframe = self._positions_dataframe[self._positions_dataframe['transactions'].notna()]
    #
    #     self._transactions_dataframe['amount'] = self._transactions_dataframe['contract_size'] * self._transactions_dataframe['transactions']
    #     self._transactions_dataframe['price'] = self._transactions_dataframe['transaction_price']
    #
    #
    #     self._transactions_dataframe['trading_date'],self._transactions_dataframe['last_trading_date'],self._transactions_dataframe['next_trading_date'] \
    #         = DataFactory.get_trading_date_str_series(data_df=self._transactions_dataframe)
    #
    #     self._positions_dataframe['market_value'] = self._positions_dataframe['position'] * self._positions_dataframe['target_price'] * self._positions_dataframe['contract_size']
    #     self._positions_dataframe['margin'] = numpy.abs(self._positions_dataframe['market_value'] * self._positions_dataframe['margin_ratio'])
    #
    #     #确定开平仓记录 round_trips
    #     self._round_trips = self.extract_round_trips(self._transactions_dataframe)
    #
    #     #确定交易日期
    #     self._positions_dataframe['trading_date'],self._positions_dataframe['last_trading_date'],self._positions_dataframe['next_trading_date'] \
    #         = DataFactory.get_trading_date_str_series(data_df=self._positions_dataframe)
    #
    #     self._positions_dataframe.sort_index(inplace=True)
    #
    #     # trades = self._transactions_dataframe.pivot(columns='INSTRUMENT', values='QTY').fillna(0.)
    #     # tradePrices = data.pivot(index='TRADE_DATE', columns='INSTRUMENT', values='NET_COST').fillna(0.)
    #     # positions = trades.cumsum()
    #     # positionPnL = (positions.shift(1) * (prices - prices.shift(1)) * contractSizes).fillna(0.)
    #     # transactionPnL = ((trades * prices - tradePrices) * contractSizes).fillna(0.)
    #     # totalPnL = positionPnL + transactionPnL
    #
    #     #确定日终持仓，  根据开平仓记录 round_trips 截取每天存续交易，计算当天的pnl 和 daily_return
    #     pos_asset_type_dict = DataFactory.get_dataframe_dict_bycolumn(self._positions_dataframe,column='asset_type')
    #     positions_daily_list= []
    #     for asset_type, pos_asset_type_df in pos_asset_type_dict.items():
    #         pos_symbol_dict = DataFactory.get_dataframe_dict_bycolumn(pos_asset_type_df,column='symbol')
    #         # round_trips_id_pnl={}
    #         for symbol, pos_df in pos_symbol_dict.items():
    #         #     last_cost_price = 0
    #
    #             market_data = DataFactory()\
    #                 .get_market_data(asset_type=asset_type,
    #                                  freq=FREQ_1D,
    #                                  symbols=[symbol],
    #                                  start_date=pos_df.iloc[0]['trading_date'],
    #                                  end_date=pos_df.iloc[-1]['trading_date']
    #                                                              )
    #
    #             datadf = market_data[symbol]
    #             assert datadf is not None
    #
    #             datadf['last_close_price'] = datadf['CLOSE_PX'].shift(1)
    #
    #             positions_daily = pos_df.groupby('trading_date').last()
    #             positions_daily.index = pandas.to_datetime(positions_daily.index + ' 15:00:00').tz_localize(DEFAULT_TIMEZONE)
    #
    #             #根据目标持仓期望得到日终头寸记录， 由于可能存在交易日没有目标持仓记录，导致缺失日期， 使用市场交易日填补
    #             trading_date_dataframe = DataFactory().get_trading_date(start_date=pos_df.iloc[0]['trading_date'],
    #                                                                     end_date=pos_df.iloc[-1]['trading_date'])
    #
    #             trading_date_dataframe.index = pandas.to_datetime(trading_date_dataframe.index.strftime('%Y-%m-%d')  + ' 15:00:00').tz_localize(DEFAULT_TIMEZONE)
    #             positions_daily = pandas.merge(positions_daily,
    #                                            trading_date_dataframe[['last_trading_date']],
    #                                            how='right',left_index=True,right_index=True,suffixes=('_del',''))
    #             if 'last_trading_date_del' in positions_daily.columns:
    #                 positions_daily.drop(columns=['last_trading_date_del'],inplace=True)
    #
    #             #遍历日期， 将每日存续交易提取出来，计算当日盈亏
    #             positions_daily['daily_return'] = 0
    #             positions_daily['daily_pnl'] = 0
    #             datadf.index = pandas.to_datetime(datadf.index.strftime('%Y-%m-%d')  + ' 15:00:00').tz_localize(DEFAULT_TIMEZONE)
    #             positions_daily = pandas.merge(positions_daily,datadf,how='left',right_index=True,left_index=True,suffixes=('','_md'))
    #
    #             for idx,row in positions_daily.iterrows():
    #                 last_trading_date = pandas.to_datetime(row['last_trading_date'].strftime('%Y-%m-%d') + ' 15:01:00').tz_localize(DEFAULT_TIMEZONE)
    #                 trading_date = pandas.to_datetime(idx.strftime('%Y-%m-%d') + ' 15:00:00').tz_localize(DEFAULT_TIMEZONE)
    #
    #                 price = row['target_price']
    #                 last_close_price = row['last_close_price']
    #                 close_price = row['CLOSE_PX']
    #                 #last_trading_date到trading_date期间，存续的交易
    #                 sub_round_trips = self._round_trips.loc[
    #                     (self._round_trips['symbol'] == symbol)
    #                     & (self._round_trips['close_dt'] >= last_trading_date)
    #                     & (self._round_trips['open_dt'] <= trading_date )
    #                 ]
    #                 if sub_round_trips.empty:
    #                     daily_return = 0
    #                     daily_pnl = 0
    #                 else:
    #                     sub_round_trips.loc[ sub_round_trips['open_dt'] < last_trading_date,'open_price' ] =  numpy.sign(sub_round_trips['open_price']) * last_close_price
    #                     sub_round_trips.loc[ sub_round_trips['close_dt'] > trading_date,'close_price' ] = numpy.sign(sub_round_trips['close_price']) * close_price
    #
    #                     pnl = -(sub_round_trips['close_price'] + sub_round_trips['open_price']) * sub_round_trips['abs_amount']
    #                     daily_pnl = pnl.sum()
    #                     daily_return = daily_pnl / numpy.abs(sub_round_trips['open_price'] * sub_round_trips['abs_amount']).sum()
    #
    #                     # for round_trips_id, rt_row in sub_round_trips.iterrows():
    #                     #     if round_trips_id not in round_trips_id_pnl:
    #                     #         round_trips_id_pnl[round_trips_id] = []
    #                     #     round_trips_id_pnl[round_trips_id].append(-(rt_row['close_price'] + rt_row['open_price']) * rt_row['abs_amount'])
    #                 positions_daily.loc[idx,'daily_return'] = daily_return
    #                 positions_daily.loc[idx,'daily_pnl'] = daily_pnl
    #             positions_daily_list.append(positions_daily)
    #
    #         #核对代码， 确认每笔开平仓收益与daily_pos是否一致，正式代码注释掉；
    #         # for round_trips_id, rt_row in self._round_trips.iterrows():
    #         #     if abs(numpy.sum(round_trips_id_pnl[round_trips_id]) -  rt_row['pnl']) > 0.00001:
    #         #         print(rt_row)
    #         #         print(round_trips_id_pnl[round_trips_id])
    #         #         assert False
    #     self._positions_daily_dataframe = pandas.concat(positions_daily_list)[[
    #         'asset_type','symbol','position','daily_pnl','daily_return','market_value'
    #         ,'contract_size','tick_size','margin_ratio','freq','remark']]
    #     self._positions_daily_dataframe.index = pandas.to_datetime(self._positions_daily_dataframe.index)
    #
    #     return (self.daily_return,self.daily_positions)

    @staticmethod
    def _gen_trading_date_index(datadf,isdaily=False):
        datadf['trading_date'], datadf['last_trading_date'], datadf[
            'next_trading_date'] = DataFactory.get_trading_date_str_series(data_df=datadf,isdaily=isdaily)
        datadf['trading_date'] = (pandas.to_datetime (pandas.DatetimeIndex(datadf['trading_date']).date) + datetime.timedelta(hours=15)).tz_localize(
            DEFAULT_TIMEZONE)
        datadf.set_index('trading_date', inplace=True)
        return datadf

    @staticmethod
    @runing_time
    def _process_settle_table(settle_table,asset_type,contract_size,tick_size,product_id,freq,symbol):
        settle_table.rename(columns={'amount':'trades','net_cost':'tradePrices','close':'close_price'},inplace=True)
        # settle_table.columns = ['trades', 'fee', 'bidAskDollar', 'tradePrices', 'last_close', 'close_price']
        # settle_table['trading_date'] = settle_table.index
        # settle_table['asset_type'] = asset_type
        # settle_table['contract_size'] = contract_size
        # settle_table['tick_size'] = tick_size
        # settle_table['product_id'] = product_id
        # settle_table['margin_ratio'] = 0.1
        # settle_table['freq'] = freq
        # settle_table['symbol'] = symbol

        settle_table['position'] = settle_table['trades'].cumsum()
        settle_table['last_position'] = settle_table['position'].shift(1).fillna(0.)

        positionPnL = (
                    settle_table['last_position'] * (settle_table['close_price'] - settle_table['last_close'])).fillna(
            0.)
        transactionPnL = ((settle_table['trades'] * settle_table['close_price'] - settle_table['tradePrices'])).fillna(
            0.)
        settle_table['position_pnL'] = positionPnL
        settle_table['transaction_pnL'] = transactionPnL
        settle_table['daily_pnl_gross'] = positionPnL + transactionPnL
        settle_table['daily_pnl_fee'] = settle_table['daily_pnl_gross'] - settle_table['fee']
        settle_table['daily_pnl'] = settle_table['daily_pnl_fee'] - settle_table['bidAskDollar']
        settle_table['market_value'] = settle_table['position'] * settle_table['close_price'] * settle_table[
            'contract_size']
        settle_table.index.name = 'datetime_index'
        return settle_table
    @runing_time
    def settle(self,positions_dataframe,bidAskDollar = 0.5):
        # 向量计算
        #ToDo,  计算每日return=pnl/(前日持仓+盘中开仓）, 盘中开仓部分如何计算？
        self._positions_dataframe = positions_dataframe

        #确定交易日期
        if 'trade_date' in self._positions_dataframe.columns:
            self._positions_dataframe['trading_date'] = self._positions_dataframe['trade_date']
        else:
            self._positions_dataframe['trading_date'],self._positions_dataframe['last_trading_date'],self._positions_dataframe['next_trading_date'] \
                = DataFactory.get_trading_date_str_series(data_df=self._positions_dataframe)
        self._positions_dataframe['trading_date'] = (pandas.to_datetime (pandas.DatetimeIndex(self._positions_dataframe['trading_date']).date) + datetime.timedelta(hours=15)).tz_localize(DEFAULT_TIMEZONE)
        t1 = time.clock()


        contract_info_df = DataFactory._get_instruments()[
            ['VolumeMultiple', 'PriceTick', 'ProductID', 'ShortMarginRatio', 'LongMarginRatio']]
        contract_info_df.reset_index(inplace=True)
        contract_info_df.set_index('symbol', inplace=True)
        contract_info_df.rename(
            columns={'VolumeMultiple': 'contract_size', 'PriceTick': 'tick_size', 'ProductID': 'product_id',
                     'ShortMarginRatio': 'margin_ratio'}, inplace=True)
        print(1,time.clock() - t1)
        t1 = time.clock()
        self._positions_dataframe['margin_ratio'] = 0.1
        # self._positions_dataframe['contract_size'] = numpy.nan
        self._positions_dataframe = pandas.merge(self._positions_dataframe,contract_info_df,how='left',left_on='symbol',right_index=True,suffixes=('_info',''))

        self._positions_dataframe['margin_ratio'].fillna(0.1,inplace=True)
        self._positions_dataframe['tick_size'] = self._positions_dataframe[ 'tick_size'].astype(numpy.float)
        self._positions_dataframe['contract_size'] = self._positions_dataframe[ 'contract_size'].astype(numpy.int64)
        self._positions_dataframe['margin_ratio'] = self._positions_dataframe[ 'margin_ratio'].astype(numpy.float)

        print(2,time.clock() - t1)
        t1 = time.clock()
        #确定交易流水 transactions
        self._transactions_dataframe = self._positions_dataframe[self._positions_dataframe['transactions'].notna()].copy()
        self._transactions_dataframe['bidAskDollar'] = numpy.abs( self._transactions_dataframe['transactions']
                                                                  *self._transactions_dataframe['contract_size']
                                                                  * self._transactions_dataframe['tick_size']
                                                                  * bidAskDollar)
        self._transactions_dataframe['fee'] = self._fee_func.calc_fee_ex(self._transactions_dataframe)
        self._transactions_dataframe['amount'] = self._transactions_dataframe['contract_size'] * numpy.round(self._transactions_dataframe['transactions'])

        print(3,time.clock() - t1)
        t1 = time.clock()
        #暂时保留非整数的交易
        # self._transactions_dataframe = self._transactions_dataframe[ numpy.abs(self._transactions_dataframe['amount']) >= 1.0 ]

        self._transactions_dataframe['price'] = self._transactions_dataframe['transaction_price']
        self._transactions_dataframe['net_cost'] = self._transactions_dataframe['amount'] * self._transactions_dataframe['price']

        self._positions_dataframe['market_value'] = self._positions_dataframe['position'] * self._positions_dataframe['target_price'] * self._positions_dataframe['contract_size']
        self._positions_dataframe['margin'] = numpy.abs(self._positions_dataframe['market_value'] * self._positions_dataframe['margin_ratio'])

        print(4,time.clock() - t1)
        t1 = time.clock()
        positions_daily_list = []
        #确定日终持仓，  根据开平仓记录 round_trips 截取每天存续交易，计算当天的pnl 和 daily_return

        print(4.1,time.clock() - t1)
        t1 = time.clock()
        pivot_assettype_symbol_freq = self._transactions_dataframe.groupby(['asset_type','freq','symbol','trading_date'])[['amount', 'fee', 'bidAskDollar','net_cost']].sum()
        pivot_assettype_symbol_freq.reset_index(inplace=True)
        pivot_assettype_symbol_freq.fillna(0.,inplace=True)
        pivot_assettype_symbol_freq = pandas.merge(pivot_assettype_symbol_freq,contract_info_df,how='left',left_on='symbol',right_index=True,suffixes=('_info',''))

        pivot_assettype_symbol_freq['margin_ratio'].fillna(0.1,inplace=True)
        pivot_assettype_symbol_freq['tick_size'] = pivot_assettype_symbol_freq[ 'tick_size'].astype(numpy.float)
        pivot_assettype_symbol_freq['contract_size'] = pivot_assettype_symbol_freq[ 'contract_size'].astype(numpy.int64)
        pivot_assettype_symbol_freq['margin_ratio'] = pivot_assettype_symbol_freq[ 'margin_ratio'].astype(numpy.float)

        market_data_df_list = []
        tmp = self._positions_dataframe[['asset_type','symbol']]
        tmp.drop_duplicates(inplace=True)
        tmp = tmp.pivot(columns='asset_type')
        tmp = tmp.to_dict(orient='list')
        for asset_type,symbols in tmp.items():
            market_data = DataFactory() \
                .get_market_data(asset_type=asset_type[1],
                                 freq=FREQ_1D,
                                 symbols=symbols,
                                 start_date=pivot_assettype_symbol_freq.iloc[0]['trading_date'],
                                 end_date=pivot_assettype_symbol_freq.iloc[-1]['trading_date'],
                                 price_type=self._price_type
                                 )
            market_data_df_list.extend([market_data[symbol, FREQ_1D, self._price_type][['last_close','close']] for symbol in symbols])

        pivot_assettype_symbol_freq = pandas.merge(pivot_assettype_symbol_freq, pandas.concat(market_data_df_list), how='left', left_on='trading_date', right_index=True)

        pivot_assettype_symbol_freq.rename(columns={'amount': 'trades', 'net_cost': 'tradePrices','close':'close_price'},inplace=True)
        groupby_tmp = pivot_assettype_symbol_freq.groupby(['asset_type','freq','symbol'])
        pivot_assettype_symbol_freq['position'] = groupby_tmp ['trades'].cumsum()
        pivot_assettype_symbol_freq['last_position'] = groupby_tmp ['position'].shift(1).fillna(0.)

        positionPnL = (pivot_assettype_symbol_freq['last_position'] * (pivot_assettype_symbol_freq['close_price'] - pivot_assettype_symbol_freq['last_close'])).fillna(0.)
        transactionPnL = ((pivot_assettype_symbol_freq['trades'] * pivot_assettype_symbol_freq['close_price'] - pivot_assettype_symbol_freq['tradePrices'])).fillna(0.)
        pivot_assettype_symbol_freq['position_pnL'] = positionPnL
        pivot_assettype_symbol_freq['transaction_pnL'] = transactionPnL
        pivot_assettype_symbol_freq['daily_pnl_gross'] = positionPnL + transactionPnL
        pivot_assettype_symbol_freq['daily_pnl_fee'] = pivot_assettype_symbol_freq['daily_pnl_gross'] - pivot_assettype_symbol_freq['fee']
        pivot_assettype_symbol_freq['daily_pnl'] = pivot_assettype_symbol_freq['daily_pnl_fee'] - pivot_assettype_symbol_freq['bidAskDollar']
        pivot_assettype_symbol_freq['market_value'] = pivot_assettype_symbol_freq['position'] * pivot_assettype_symbol_freq['close_price'] * pivot_assettype_symbol_freq['contract_size']
        pivot_assettype_symbol_freq['datetime_index'] = pivot_assettype_symbol_freq['trading_date']
        pivot_assettype_symbol_freq.set_index('datetime_index',inplace=True)

        self._positions_daily_dataframe = pivot_assettype_symbol_freq[[
            'asset_type','symbol','position','last_position','close_price','last_close'
            ,'daily_pnl','daily_pnl_gross','daily_pnl_fee','fee','bidAskDollar'
            ,'position_pnL','transaction_pnL','market_value'
            ,'contract_size','tick_size','margin_ratio','freq','trading_date']]
        return (self.daily_return, self.daily_positions)
        # print(4.1,time.clock() - t1)
        # t1 = time.clock()
        # pivot_assettype_symbol_freq = self._transactions_dataframe.pivot_table(index='trading_date',
        #                                                                                columns=['asset_type','freq','symbol'],
        #                                                                                values=['amount', 'fee', 'bidAskDollar','net_cost'],
        #                                                                   aggfunc=numpy.sum).fillna(0.)
        # print(4.2,time.clock() - t1)
        # t1 = time.clock()
        # for asset_type in pivot_assettype_symbol_freq.columns.levels[1]:
        #     pivot_symbol_freq = pivot_assettype_symbol_freq.xs(asset_type,level=1,axis=1)
        #     symbols = list(pivot_symbol_freq.columns.levels[2])
        #
        #     market_data = DataFactory() \
        #         .get_market_data(asset_type=asset_type,
        #                          freq=FREQ_1D,
        #                          symbols=symbols,
        #                          start_date=pivot_symbol_freq.iloc[0].name,
        #                          end_date=pivot_symbol_freq.iloc[-1].name,
        #                          price_type=self._price_type
        #                          )
        #     _gen_trading_date_index_daily = partial(Settlement._gen_trading_date_index,isdaily=True)
        #     for freq in pivot_symbol_freq.columns.levels[1]:
        #         pivot_symbol = pivot_symbol_freq.xs(freq,level=1,axis=1)
        #         symbol_list = list(pivot_symbol.columns.levels[1])
        #
        #         trades_fee_bidAskDollar_tradePrices_list = [pivot_symbol.xs(symbol,level=1,axis=1) for symbol in symbol_list]
        #         datadf_list = [market_data[symbol,FREQ_1D,self._price_type][['last_close','close']]  for symbol in symbol_list]
        #
        #         # datadf_list = list(map(_gen_trading_date_index_daily,datadf_list))
        #         settle_table_list = list(map(lambda x: pandas.merge(x[0],x[1][['last_close','close']],how='outer',left_index=True,right_index=True),zip(trades_fee_bidAskDollar_tradePrices_list,datadf_list)))
        #         settle_table_list = list(map(Settlement._process_settle_table,
        #                                      settle_table_list,
        #                                          [asset_type]*len(symbol_list),
        #                                          [contract_info_df.loc[symbol]['contract_size'] for symbol in symbol_list] ,
        #                                          [contract_info_df.loc[symbol]['tick_size'] for symbol in symbol_list] ,
        #                                          [contract_info_df.loc[symbol]['product_id'] for symbol in symbol_list] ,
        #                                          [freq]*len(symbol_list),
        #                                          symbol_list
        #                                      ))
        #         positions_daily_list.extend(settle_table_list)
        #
        #     #核对代码， 确认每笔开平仓收益与daily_pos是否一致，正式代码注释掉；
        #     # for round_trips_id, rt_row in self._round_trips.iterrows():
        #     #     if abs(numpy.sum(round_trips_id_pnl[round_trips_id]) -  rt_row['pnl']) > 0.00001:
        #     #         print(rt_row)
        #     #         print(round_trips_id_pnl[round_trips_id])
        #     #         assert False
        # self._positions_daily_dataframe = pandas.concat(positions_daily_list)[[
        #     'asset_type','symbol','position','last_position','close_price','last_close'
        #     ,'daily_pnl','daily_pnl_gross','daily_pnl_fee','fee','bidAskDollar'
        #     ,'position_pnL','transaction_pnL','market_value'
        #     ,'contract_size','tick_size','margin_ratio','freq','trading_date']]
        # print(5,time.clock() - t1)
        # t1 = time.clock()
        # return (self.daily_return,self.daily_positions)

    @property
    def transactions(self):
        return self._transactions_dataframe

    @property
    def round_trips(self):
        return self._round_trips

    @property
    def daily_positions(self):
        if self._positions_daily_dataframe is None:
            return  None
        return self._positions_daily_dataframe

    @property
    def daily_pnl_gross(self):
        daily_pnl = self._positions_daily_dataframe.pivot_table(index='trading_date',
                                                                values='daily_pnl_gross', aggfunc=numpy.sum).fillna(0.)
        return daily_pnl
    @property
    def daily_pnl_fee(self):
        daily_pnl = self._positions_daily_dataframe.pivot_table(index='trading_date',
                                                                values='daily_pnl_fee', aggfunc=numpy.sum).fillna(0.)
        return daily_pnl
    @property
    def daily_pnl(self):
        daily_pnl = self._positions_daily_dataframe.pivot_table(index='trading_date',
                                                                values='daily_pnl', aggfunc=numpy.sum).fillna(0.)
        return daily_pnl
    @property
    def daily_return(self):
        if self._positions_daily_dataframe is None or\
                'daily_pnl' not in self._positions_daily_dataframe.columns:
            return  None
        daily_pnl = self.daily_pnl
        daily_pnl.name = 'pnl'
        daily_return_df = pandas.DataFrame(daily_pnl)
        daily_return_df.columns = ['pnl']
        daily_return_df['pnl_cumsum'] = daily_return_df['pnl'].cumsum()
        daily_return_df['aum'] = self._init_aum + daily_return_df['pnl_cumsum']
        daily_return_df['daily_return'] = (daily_return_df['pnl'] / daily_return_df['aum']).fillna(0.)
        daily_return_df.index = (pandas.to_datetime (pandas.DatetimeIndex(daily_return_df.index).date) + datetime.timedelta(hours=15)).tz_localize(DEFAULT_TIMEZONE)
        daily_return_df.sort_index(inplace=True)
        return daily_return_df['daily_return']

        # daily_cost = self._positions_daily_dataframe.pivot_table(index='trading_date',
        #                                                            values='daily_cost', aggfunc=numpy.sum).fillna(0.)
        # daily_return = pandas.concat(
        #     [daily_pnl['daily_pnl'], daily_cost['daily_cost']], axis=1).fillna(0.)
        # daily_return.columns = ['daily_pnl', 'daily_cost']
        # daily_return['daily_return'] = (daily_return['daily_pnl'] / daily_return['daily_cost']).fillna(0.)
        # daily_return.index = pandas.to_datetime(daily_return.index+ ' 15:00:00').tz_localize(DEFAULT_TIMEZONE)
        # daily_return.sort_index(inplace=True)
        # return daily_return['daily_return']

if __name__ == '__main__':
    from analysis.analysis import Analysis
    #临时脚本
    from execution.execution import Execution

    # signal_dataframe = pandas.DataFrame.from_csv('E:\\tstf_intraday\\AG' + '//signal_dataframe.csv')
    # if signal_dataframe.index.tzinfo is None:
    #     signal_dataframe.index = signal_dataframe.index.tz_localize(DEFAULT_TIMEZONE)
    # execution_obj = Execution(freq=FREQ_1M, exec_price_mode=Execution.EXEC_BY_CLOSE, exec_lag=1)
    # (success, positions_dataframe) = execution_obj.exec_trading(signal_dataframe=signal_dataframe)
    # positions_dataframe.to_csv('E:\\tstf_intraday\\AG' + '//positions_dataframe.csv')
    positions_dataframe = pandas.DataFrame.from_csv(r'E:\PairStrategy\resRepo_compair_all_C_VOL_M_VOL_1M_exec_1\positions_dataframe.csv')
    settlement_obj = Settlement()
    settlement_obj.settle(positions_dataframe)
    markets = ['C', 'A', 'Y', 'M', 'P', 'L', 'V', 'J', 'JM', 'JD', 'I', 'PP', 'CS',
               'AP', 'CF', 'FG', 'MA', 'OI', 'RM', 'SF', 'SM', 'SR', 'TA', 'WH', 'ZC',
               'AG', 'AL', 'AU', 'CU', 'NI', 'RB', 'RU', 'ZN',
               'BU', 'HC', 'SN']

    analysis_obj = Analysis(daily_returns=settlement_obj.daily_return,
                                    daily_positions=settlement_obj.daily_positions,
                                    transactions=settlement_obj.transactions,
                                    round_trips=settlement_obj.round_trips,
                result_folder='e://tstf_intraday' ,strategy_id='tsf_intraday_' + '_'.join(markets)
                                ,symbols=markets
                                ,strategy_type='tsf_intraday')
    analysis_obj.plot_all()
    analysis_obj.save_result()
    #分析保存结果
    # 分析保存结果
    # print(('cum_returns', analysis_obj.cum_returns()))
    # print(('max_drawdown', analysis_obj.max_drawdown()))
    # print(('sharpe_ratio', analysis_obj.sharpe_ratio()))
    # print(('sortino_ratio', analysis_obj.sortino_ratio()))
    # print(('get_long_short_pos', analysis_obj.get_long_short_pos()))
    # print(('get_max_median_position_concentration', analysis_obj.get_max_median_position_concentration()))
    # print(('get_top_long_short_abs', analysis_obj.get_top_long_short_abs()))
    #
    # analysis_obj.plot_holdings(show=False)
    # analysis_obj.plot_rolling_returns(show=False)
    # analysis_obj.plot_returns(show=False)
    # analysis_obj.plot_monthly_returns_heatmap(show=False)
    # # analysis_obj.plot_drawdown_periods(show=False)
    # analysis_obj.plot_monthly_returns_dist(show=False)
    # analysis_obj.plot_txn_time_hist(show=False)
    # analysis_obj.plot_round_trip_lifetimes(show=False)