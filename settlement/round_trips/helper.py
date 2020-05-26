import pandas
import numpy

from multiprocessing import Pool,cpu_count
from settlement.round_trips.trade_item import TradeItem, BuyItem, SellItem
from settlement.round_trips.signal import Signal, LongSignal, ShortSignal, EmptySignal, in_same_round_trip
from settlement.round_trips.round_trip import RoundTrip
from settlement.round_trips.round_trip_group import RoundTripHistory

def trade_items_list_to_round_trip_his(trade_items):
    # from settlement.round_trips.round_trip_group import RoundTripHistory
    return RoundTripHistory(trade_items)

def load_by_transactions(transactions):
    import pandas
    import numpy

    assert isinstance(transactions, pandas.DataFrame)
    dataframe = transactions.sort_index()

    assert 'transactions' in dataframe.columns \
           and 'transaction_price' in dataframe.columns \
           and ('contract_id' in dataframe.columns or 'symbol' in dataframe.columns)
    col = 'symbol'
    if 'contract_id' in dataframe.columns:
        col = 'contract_id'
    symbol_df_list = [(symbol, df) for symbol, df in dataframe.groupby(col)]
    df_list = [df for _, df in symbol_df_list]

    cpu = cpu_count() - 1
    cpu = max(1, min(cpu, 12))
    pool = Pool(cpu)

    # print('_df_to_trade_items_list', cpu)
    trade_items_list = pool.map(RoundTripHistory._df_to_trade_items_list, df_list)
    pool.close()
    pool.join()
    # print('_df_to_trade_items_list, done')
    # print('RoundTripHistory_dict', cpu)
    # ret_list = [RoundTripHistory(y) for y in trade_items_list]
    pool.terminate()
    del pool

    ret_list = []
    pool = Pool(cpu)
    result_list =[]
    for y in trade_items_list:
        # ret_list.append(trade_items_list_to_round_trip_his(y))
        result_list.append(pool.apply_async(trade_items_list_to_round_trip_his,args=(y,)))
    pool.close()
    # print('RoundTripHistory_dict, done')
    ret_list = [x.get() for x in result_list]
    del result_list
    pool.terminate()
    del pool
    ret_dict = {}
    for x, y in zip(symbol_df_list, ret_list):
        ret_dict[x[0]] = y
    return ret_dict