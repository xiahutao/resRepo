# -*- coding: utf-8 -*-

import pandas
import numpy
from data_engine.data_factory import DataFactory
from data_engine.setting import DATASOURCE_LOCAL,DATASOURCE_REMOTE
from data_engine.setting import FREQ_1M,FREQ_1D,FREQ_TICK,DEFAULT_TIMEZONE
import data_engine.setting as setting
from common.decorator import runing_time
from common.mult_threading import MyThread
from config.config import Config_back_test
from execution.execution import Execution
from data_engine.instrument.future import Future

class Execution_ex(Execution):
    def __init__(self,config):
        self._config = config
        assert isinstance(config,Config_back_test)
        freq = config.get_data_config('freq')
        if 'freq' in config.execution_config:
            freq = config.get_execution_config('freq')
        Execution.__init__(self,freq=freq,exec_price_mode=config.get_execution_config('exec_price_mode'),exec_lag=config.get_execution_config('exec_lag'),price_type=config.get_data_config('price_type'))

    @staticmethod
    def _exec_trading_df(sub_signal_df,price_df,price_after_slipp_df,remark,_exec_lag=1):

        if price_df is None or price_after_slipp_df is None:
            return None
        #合并信号点价格，和指定成交价格
        price_df = pandas.merge(price_df,price_after_slipp_df,how='outer',right_index=True,left_index=True,suffixes=('','_after_slipp'))
        sub_signal_df = pandas.merge(sub_signal_df, price_df, how='right', right_index=True, left_index=True)
        cols = [x for x in sub_signal_df.columns if x not in price_df.columns]
        sub_signal_df = pandas.merge(sub_signal_df[cols].shift(_exec_lag), price_df, how='right', right_index=True, left_index=True)
        sub_signal_df = sub_signal_df[sub_signal_df['symbol'].notna()]
        if sub_signal_df.index.tzinfo is None:
            sub_signal_df.index = sub_signal_df.index.tz_localize(DEFAULT_TIMEZONE)
        else:
            sub_signal_df.index = sub_signal_df.index.tz_convert(DEFAULT_TIMEZONE)
        # sub_signal_df['target_price'].ffill(inplace=True)
        # sub_signal_df['price_after_slipp'].ffill(inplace=True)

        #成交价格，和信号时刻价格偏差
        sub_signal_df['slipp'] = sub_signal_df['target_price'] - sub_signal_df['price_after_slipp']
        # sub_signal_df['fee_per_pos'] = 0

        sub_signal_df['position'] = numpy.round( numpy.abs(sub_signal_df['position']) ,5) * numpy.sign(sub_signal_df['position'])
        # 计算交易部分 transactions， transactions_close平仓，transactions_open开仓  ========= todo 平仓开仓部分可以考虑去掉了
        sub_signal_df['last_position'] = sub_signal_df['position'].shift(1).fillna(0)

        # sub_signal_df['transactions_open'] = numpy.nan
        # sub_signal_df['transactions_close'] = numpy.nan
        #
        # # 开多仓（加多仓）
        # sub_signal_df_open_long = sub_signal_df.loc[
        #     (sub_signal_df['last_position'] >= 0) & (sub_signal_df['position'] > sub_signal_df['last_position'])]
        # sub_signal_df_open_long['transactions_open'] = sub_signal_df_open_long['position'] - sub_signal_df_open_long[
        #     'last_position']
        # sub_signal_df.loc[
        #     (sub_signal_df['last_position'] >= 0) & (
        #                 sub_signal_df['position'] > sub_signal_df['last_position']), 'transactions_open'] = \
        # sub_signal_df_open_long['transactions_open']
        #
        # # 开空仓（加空仓）
        # sub_signal_df_open_short = sub_signal_df.loc[
        #     (sub_signal_df['last_position'] <= 0) & (sub_signal_df['position'] < sub_signal_df['last_position'])]
        # sub_signal_df_open_short['transactions_open'] = sub_signal_df_open_short['position'] - sub_signal_df_open_short[
        #     'last_position']
        # sub_signal_df.loc[
        #     (sub_signal_df['last_position'] <= 0) & (
        #                 sub_signal_df['position'] < sub_signal_df['last_position']), 'transactions_open'] = \
        # sub_signal_df_open_short['transactions_open']
        #
        # # 平多仓（反手空仓）
        # sub_signal_df_close_long = sub_signal_df.loc[
        #     (sub_signal_df['last_position'] > 0) & (sub_signal_df['position'] < sub_signal_df['last_position'])]
        # sub_signal_df_close_long['transactions_close'] = - sub_signal_df_close_long['last_position'] + \
        #                                                  sub_signal_df_close_long['position'].where(
        #                                                      sub_signal_df_close_long['position'] > 0, 0)
        # sub_signal_df_close_long['transactions_open'] = sub_signal_df_close_long['position'].where(
        #     sub_signal_df_close_long['position'] < 0, 0)
        # sub_signal_df.loc[
        #     (sub_signal_df['last_position'] > 0) & (
        #                 sub_signal_df['position'] < sub_signal_df['last_position']), 'transactions_close'] = \
        # sub_signal_df_close_long['transactions_close']
        # sub_signal_df.loc[
        #     (sub_signal_df['last_position'] > 0) & (
        #                 sub_signal_df['position'] < sub_signal_df['last_position']), 'transactions_open'] = \
        # sub_signal_df_close_long['transactions_open']
        #
        # # 平空仓（反手多仓）
        # sub_signal_df_close_short = sub_signal_df.loc[
        #     (sub_signal_df['last_position'] < 0) & (sub_signal_df['position'] > sub_signal_df['last_position'])]
        # sub_signal_df_close_short['transactions_close'] = - sub_signal_df_close_short['last_position'] + \
        #                                                   sub_signal_df_close_short['position'].where(
        #                                                       sub_signal_df_close_short['position'] < 0, 0)
        # sub_signal_df_close_short['transactions_open'] = sub_signal_df_close_short['position'].where(
        #     sub_signal_df_close_short['position'] > 0, 0)
        # sub_signal_df.loc[
        #     (sub_signal_df['last_position'] < 0) & (
        #                 sub_signal_df['position'] > sub_signal_df['last_position']), 'transactions_close'] = \
        # sub_signal_df_close_short['transactions_close']
        # sub_signal_df.loc[
        #     (sub_signal_df['last_position'] < 0) & (
        #                 sub_signal_df['position'] > sub_signal_df['last_position']), 'transactions_open'] = \
        # sub_signal_df_close_short['transactions_open']

        sub_signal_df['transactions'] = sub_signal_df['position'] - sub_signal_df['last_position']
        sub_signal_df['transactions'].replace(0, numpy.nan, inplace=True)
        # sub_signal_df['transactions_close'].replace(0, numpy.nan, inplace=True)
        # sub_signal_df['transactions_open'].replace(0, numpy.nan, inplace=True)
        sub_signal_df['transaction_price'] = sub_signal_df['price_after_slipp']
        sub_signal_df.loc[sub_signal_df['transactions'].isna(), 'transaction_price'] = numpy.nan

        sub_signal_df['remark'] = remark
        return sub_signal_df

    def _get_market_data(self,asset_type,x,y):
        market_data = self._data_factory.get_market_data(asset_type=asset_type, freq=self._freq,
                                           symbols=x,
                                           start_date=y.index.min(),
                                           end_date=y.index.max(),
                                           price_type=self._price_type
                                           )
        df = market_data[x, self._freq, self._price_type]
        market_data.pop((x, self._freq, self._price_type))
        return (x,df)


    @runing_time
    def exec_trading(self,signal_dataframe):
        if signal_dataframe is None or signal_dataframe.empty:
            return (False,'signal_dataframe is none or empty')
        if 'asset_type' not in signal_dataframe.columns or  'symbol' not in signal_dataframe.columns:
            return (False,'missing asset_type/symbol column')

        if signal_dataframe.index.tzinfo is None:
            signal_dataframe.index = signal_dataframe.index.tz_localize(DEFAULT_TIMEZONE)
        else:
            signal_dataframe.index = signal_dataframe.index.tz_convert(DEFAULT_TIMEZONE)
        if 'remark' not in signal_dataframe.columns:
            signal_dataframe['remark'] = ''

        if 'contract_id' in signal_dataframe.columns and 'cpt_instrument' not in signal_dataframe:
            signal_dataframe['cpt_instrument'] = None
            contract_idlist = set(list(signal_dataframe['contract_id']))
            for contract_id in  contract_idlist:
                fut = Future(symbol=contract_id)
                signal_dataframe.loc[signal_dataframe['contract_id'] == contract_id,'cpt_instrument'] = fut.ctp_symbol

        sub_signal_df_list = []
        market_data_dict = {}
        # 对不同asset_type分组， 可以支持 股票、期货等不同品种在一起撮合
        if False and 'contract_id' in signal_dataframe.columns:
            for asset_type, sub_signal_df_asset_type in signal_dataframe.groupby(['asset_type']):
                symbol_list = list(sub_signal_df_asset_type['contract_id'].unique())
                # 提取撮合频率下的量价数据
                ts = []
                df_list={}
                mt = MyThread()
                g_func_list = []
                contract_id_list = []
                sub_df_tmp_list = []
                for contract_id, sub_df_tmp in sub_signal_df_asset_type.groupby('contract_id'):
                    contract_id_list.append(contract_id)
                    sub_df_tmp_list.append(sub_df_tmp)
                    # g_func_list.append({"func": self._get_market_data, "args": (asset_type,contract_id,sub_df_tmp)})
                    # contract_id_list.append(contract_id)
                # mt.set_thread_func_list(func_list=g_func_list)
                # mt.start()

                df_list = map(self._get_market_data,[asset_type]*len(contract_id_list), contract_id_list,sub_df_tmp_list)
                # df_list = {x:y for x,y in zip(contract_id_list,mt.result_list)}
                df_list = {x:y for x,y in df_list}
                # 根据remark， 分组， 支持类似与多个Pair一起进行交易的情况
                for remark, sub_signal_df_remark in sub_signal_df_asset_type.groupby(['remark']):
                    symbol_remark_list = list(sub_signal_df_remark['contract_id'].unique())
                    datadf_list = [df_list[symbol] for symbol in symbol_remark_list]

                    # 各个symbol的信号dataframe
                    sub_signal_df_tmp_dict = {symbol: df_tmp for symbol, df_tmp in sub_signal_df_remark.groupby(['contract_id'])}
                    # _exec_lag指定的bar价格， 例如下一个bar
                    sub_signal_df_tmp_list = [sub_signal_df_tmp_dict[symbol].shift(-self._exec_lag) for symbol in symbol_remark_list]

                    # 该信号时点的价格
                    price_df_list = list(map(self._get_price_series, datadf_list, ['target_price'] * len(datadf_list)))
                    price_after_slipp_df_list = list(map(self._get_price_series, datadf_list, ['price_after_slipp'] * len(datadf_list)))

                    price_after_slipp_df_list = list(map(lambda x: x.ffill(), price_after_slipp_df_list))

                    # map到各个symbol 执行_exec_trading_df
                    sub_signal_df_tmp_list = list(map(Execution._exec_trading_df,
                                                      sub_signal_df_tmp_list,
                                                      price_df_list, price_after_slipp_df_list,
                                                      [remark] * len(symbol_remark_list),
                                                      [self._exec_lag] * len(symbol_remark_list)
                                                      ))

                    # 合并撮合结果到sub_signal_df_list
                    sub_signal_df_tmp_list = [x for x in sub_signal_df_tmp_list if x is not None]
                    sub_signal_df_list.extend(sub_signal_df_tmp_list)
        else:
            for asset_type, sub_signal_df_asset_type in signal_dataframe.groupby(['asset_type']):
                market_data_dict = {}
                for symbol, sub_signal_df_symbol in sub_signal_df_asset_type.groupby(['symbol']):
                    market_data = self._data_factory.get_market_data(asset_type=asset_type, freq=self._freq,
                                                                     symbols=[symbol],
                                                                     start_date=sub_signal_df_symbol.index.min(),
                                                                     end_date=sub_signal_df_symbol.index.max(),
                                                                     price_type=self._price_type
                                                                     )
                    market_data_dict[symbol] = market_data

                # symbol_list = list(sub_signal_df_asset_type['symbol'].unique())
                # #提取撮合频率下的量价数据
                # market_data = self._data_factory.get_market_data(asset_type=asset_type,freq=self._freq,symbols=symbol_list,
                #                                                  start_date=sub_signal_df_asset_type.index.min(),
                #                                                  end_date=sub_signal_df_asset_type.index.max(),
                #                                                  price_type=self._price_type
                #                                                  )
                #根据remark， 分组， 支持类似与多个Pair一起进行交易的情况
                for remark, sub_signal_df_remark in sub_signal_df_asset_type.groupby(['remark']):
                    symbol_remark_list = list(sub_signal_df_remark['symbol'].unique())
                    datadf_list = [market_data_dict[symbol].get_data(symbol,price_type = self._price_type) for symbol in symbol_remark_list]

                    #各个symbol的信号dataframe
                    sub_signal_df_tmp_dict = {symbol:df_tmp for symbol,df_tmp in sub_signal_df_remark.groupby(['symbol'])}
                    sub_signal_df_tmp_list = [sub_signal_df_tmp_dict[symbol] for symbol in symbol_remark_list]
                    if self._exec_lag in [0,1]:

                        # sub_signal_df_tmp_list = [df_tmp.shift(self._exec_lag) for df_tmp in sub_signal_df_tmp_list]
                        price_df_list = list(map(self._get_price_series,datadf_list,['target_price']*len(datadf_list)))
                        price_after_slipp_df_list =  [x.rename(columns={'target_price':'price_after_slipp'})[['price_after_slipp']] for x in price_df_list ]
                    else:

                        #该信号时点的价格
                        price_df_list = list(map(self._get_price_series,datadf_list,['target_price']*len(datadf_list)))

                        #_exec_lag指定的bar价格， 例如下一个bar
                        if self._exec_lag == 0:
                            price_after_slipp_df_list =  list(map(self._get_price_series,datadf_list,['price_after_slipp']*len(datadf_list)))
                        else:
                            price_after_slipp_df_list = list(map(self._get_next_price_series,datadf_list,['price_after_slipp']*len(datadf_list)))
                    price_after_slipp_df_list = list(map(lambda x: x.ffill(), price_after_slipp_df_list))

                    #map到各个symbol 执行_exec_trading_df
                    sub_signal_df_tmp_list = list(map(Execution_ex._exec_trading_df,
                                                      sub_signal_df_tmp_list,
                                                  price_df_list, price_after_slipp_df_list, [remark]*len(symbol_remark_list)
                                                 ))

                    #合并撮合结果到sub_signal_df_list
                    sub_signal_df_tmp_list = [x for x in sub_signal_df_tmp_list if x is not None]
                    sub_signal_df_list.extend(sub_signal_df_tmp_list)

        #     market_data_dict[asset_type] = market_data
        # for (asset_type,symbol,remark), sub_signal_df in signal_dataframe.groupby(['asset_type','symbol','remark']):
        #     if sub_signal_df.empty:
        #         continue
        #     # print((asset_type,symbol,remark))
        #     market_data = market_data_dict[asset_type]
        #     datadf = market_data[symbol,self._freq,self._price_type]
        #
        #     price_df = self._get_price_series(datadf=datadf,rename='target_price')
        #     if price_df is None:
        #         continue
        #
        #     if self._exec_lag == 0:
        #         price_after_slipp_df = self._get_price_series(datadf=datadf, rename='price_after_slipp')
        #     else:
        #         price_after_slipp_df = self._get_next_price_series(datadf=datadf, rename='price_after_slipp')
        #         #todo, 有错误， datadf 是连续合约的时候， price_after_slipp 在换月时候，可能换成是下一个月份的价格，错误！
        #         assert self._exec_lag == 1
        #
        #     if price_after_slipp_df is None:
        #         continue
        #     price_after_slipp_df = price_after_slipp_df.ffill()
        #
        #     sub_signal_df = pandas.merge(sub_signal_df,price_df,how='left',right_index=True,left_index=True)
        #     sub_signal_df = pandas.merge(sub_signal_df,price_after_slipp_df,how='left',right_index=True,left_index=True)
        #     # sub_signal_df['target_price'].ffill(inplace=True)
        #     # sub_signal_df['price_after_slipp'].ffill(inplace=True)
        #     sub_signal_df['slipp'] = sub_signal_df['target_price'] - sub_signal_df['price_after_slipp']
        #     sub_signal_df['fee_per_pos'] = 0
        #
        #     sub_signal_df['last_position'] = sub_signal_df['position'].shift(1).fillna(0)
        #
        #     sub_signal_df['transactions_open'] = numpy.nan
        #     sub_signal_df['transactions_close'] = numpy.nan
        #
        #     #开多仓（加多仓）
        #     sub_signal_df_open_long = sub_signal_df.loc[(sub_signal_df['last_position'] >= 0) & (sub_signal_df['position'] > sub_signal_df['last_position'])]
        #     sub_signal_df_open_long['transactions_open'] = sub_signal_df_open_long['position'] - sub_signal_df_open_long['last_position']
        #     sub_signal_df.loc[
        #         (sub_signal_df['last_position'] >= 0) & (sub_signal_df['position'] > sub_signal_df['last_position']),'transactions_open'] = sub_signal_df_open_long['transactions_open']
        #
        #     # 开空仓（加空仓）
        #     sub_signal_df_open_short = sub_signal_df.loc[(sub_signal_df['last_position'] <= 0) & (sub_signal_df['position'] < sub_signal_df['last_position'])]
        #     sub_signal_df_open_short['transactions_open'] = sub_signal_df_open_short['position'] - sub_signal_df_open_short['last_position']
        #     sub_signal_df.loc[
        #         (sub_signal_df['last_position'] <= 0) & (sub_signal_df['position'] < sub_signal_df['last_position']),'transactions_open'] = sub_signal_df_open_short['transactions_open']
        #
        #     # 平多仓（反手空仓）
        #     sub_signal_df_close_long = sub_signal_df.loc[(sub_signal_df['last_position'] > 0) & (sub_signal_df['position'] < sub_signal_df['last_position'])]
        #     sub_signal_df_close_long['transactions_close'] = - sub_signal_df_close_long['last_position']  + sub_signal_df_close_long['position'].where(sub_signal_df_close_long['position'] >0, 0)
        #     sub_signal_df_close_long['transactions_open'] = sub_signal_df_close_long['position'].where(sub_signal_df_close_long['position'] <0, 0)
        #     sub_signal_df.loc[
        #         (sub_signal_df['last_position'] > 0) & (sub_signal_df['position'] < sub_signal_df['last_position']),'transactions_close'] = sub_signal_df_close_long['transactions_close']
        #     sub_signal_df.loc[
        #         (sub_signal_df['last_position'] > 0) & (sub_signal_df['position'] < sub_signal_df['last_position']),'transactions_open'] = sub_signal_df_close_long['transactions_open']
        #
        #     # 平空仓（反手多仓）
        #     sub_signal_df_close_short = sub_signal_df.loc[(sub_signal_df['last_position'] < 0) & (sub_signal_df['position'] > sub_signal_df['last_position'])]
        #     sub_signal_df_close_short['transactions_close'] = - sub_signal_df_close_short['last_position']  + sub_signal_df_close_short['position'].where(sub_signal_df_close_short['position'] <0, 0)
        #     sub_signal_df_close_short['transactions_open'] = sub_signal_df_close_short['position'].where(sub_signal_df_close_short['position'] >0, 0)
        #     sub_signal_df.loc[
        #         (sub_signal_df['last_position'] < 0) & (sub_signal_df['position'] > sub_signal_df['last_position']),'transactions_close'] = sub_signal_df_close_short['transactions_close']
        #     sub_signal_df.loc[
        #         (sub_signal_df['last_position'] < 0) & (sub_signal_df['position'] > sub_signal_df['last_position']),'transactions_open'] = sub_signal_df_close_short['transactions_open']
        #
        #     sub_signal_df['transactions'] = sub_signal_df['position'] - sub_signal_df['last_position']
        #     sub_signal_df['transactions'].replace(0,numpy.nan,inplace=True)
        #     sub_signal_df['transactions_close'].replace(0,numpy.nan,inplace=True)
        #     sub_signal_df['transactions_open'].replace(0,numpy.nan,inplace=True)
        #     sub_signal_df['transaction_price'] = sub_signal_df['price_after_slipp']
        #     sub_signal_df.loc[sub_signal_df['transactions'].isna(),'transaction_price'] = numpy.nan
        #
        #     sub_signal_df['remark'] = remark
        #     sub_signal_df_list.append(sub_signal_df)

        positions_dataframe = None
        if len(sub_signal_df_list)>0:
            positions_dataframe = pandas.concat(sub_signal_df_list)
        return (True,positions_dataframe)

if __name__ == '__main__':
    DataFactory.config('password',DATASOURCE_DEFAULT=DATASOURCE_LOCAL)
    exec = Execution(freq=FREQ_1M)
    signal_dataframe = pandas.DataFrame.from_csv(r'E:\PairStrategy\resRepo_compair_all_C_VOL_M_VOL_5M_exec_1\signal_dataframe.csv')
    (sucess,positions_dataframe) = exec.exec_trading(signal_dataframe=signal_dataframe)
    print(positions_dataframe)