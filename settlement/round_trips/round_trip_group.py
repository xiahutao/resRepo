import pandas
import numpy

from common.decorator import runing_time
import matplotlib.pyplot as plt
from settlement.round_trips.trade_item import TradeItem,BuyItem,SellItem
from settlement.round_trips.signal import Signal,LongSignal,ShortSignal,EmptySignal,in_same_round_trip
from settlement.round_trips.round_trip import RoundTrip

from multiprocessing import Pool,cpu_count

class RoundTripGroup(list):
    def __init__(self,round_trip=None):
        list.__init__(self,[])
        self._start_dt = None
        self._end_dt = None

        self._first = None
        self._last = None
        if round_trip is None:
            pass
        elif isinstance(round_trip,RoundTrip):
            self.append(round_trip)
        elif isinstance(round_trip,list):
            for x in round_trip:
                self.append(x)

    # def __repr__(self):
    #     return '%s ~ %s, count: %s' %(str(self._start_dt),str(self._end_dt),len(self))

    @property
    def open_dt(self):
        if self._start_dt is not None:
            return self._start_dt
        return None

    @property
    def close_dt(self):
        if self._end_dt is not None:
            return self._end_dt
        return None

    def append(self, signal,extend=True):
        if isinstance(signal,Signal):
            list.append(self,signal)
        elif isinstance(signal,RoundTripGroup) and extend:
            list.extend(self,signal)
        elif isinstance(signal,RoundTripGroup) and not extend:
            list.append(self,signal)

        if isinstance(signal, Signal) or isinstance(signal, RoundTripGroup):
            if self.open_dt is None \
                    or signal.open_dt is not None and self.open_dt > signal.open_dt:
                self._start_dt = signal.open_dt
                self._first = signal
            if self.close_dt is None \
                    or signal.close_dt is not None and self.close_dt < signal.close_dt:
                self._end_dt = signal.close_dt
                self._last = signal

    def group_by_long_short(self):
        long_group = RoundTripGroup()
        short_group = RoundTripGroup()
        for each in self:
            if isinstance(each,LongSignal):
                long_group.append(each)
            elif isinstance(each,ShortSignal):
                short_group.append(each)
        return long_group,short_group

    @staticmethod
    def _agg_by_same_round_trip(round_trip_group):
        ret = []
        if round_trip_group._last is round_trip_group._first:
            return round_trip_group
        if in_same_round_trip(round_trip_group._first,round_trip_group._last):
            return round_trip_group

        head_group = RoundTripGroup(round_trip_group._first)
        tail_group = RoundTripGroup(round_trip_group._last)
        other_group =RoundTripGroup()

        for each in round_trip_group:
            if each is round_trip_group._first:
                continue
            if each is round_trip_group._last:
                continue
            if in_same_round_trip(head_group,each):
                head_group.append(each)
            elif in_same_round_trip(tail_group,each):
                tail_group.append(each)
            else:
                other_group.append(each)

        if len(other_group)>0:
            agg_group = RoundTripGroup._agg_by_same_round_trip(other_group)
            for each in agg_group:
                if in_same_round_trip(head_group,each):
                    head_group.append(each,extend=True)
                    continue
                if in_same_round_trip(tail_group,each):
                    tail_group.append(each,extend=True)
                    continue
                ret.append(each)
        if in_same_round_trip(head_group,tail_group):
            head_group.append(tail_group)
            return head_group

        ret.append(head_group)
        ret.append(tail_group)

        return ret

    def pnl(self,contract_size=1,current_price=None):
        return sum([x.pnl(contract_size=contract_size,current_price=current_price) for x in self if isinstance(x, Signal)])

    def pos_list(self):
        return [x.open_pos() for x in self if isinstance(x, Signal)]

    def hold_return_list(self,current_price = None,margin_ratio = 1.0):
        return [x.hold_return(current_price = current_price, margin_ratio=margin_ratio) for x in self if isinstance(x, Signal)]

    def hold_period_list(self):
        return [x.hold_period for x in self if isinstance(x, Signal)]

    def daily_return_list(self,current_price = None, margin_ratio = 1.0):
        return [x.daily_return(current_price = current_price, margin_ratio=margin_ratio) for x in self if isinstance(x, Signal)]

    def pos_sum(self):
        return sum(self.pos_list())

class RoundTripHistory(object):
    def __init__(self, trade_items = None):
        self._roundtrip_closed = RoundTripGroup()
        self._roundtripl_unclosed_long = RoundTripGroup()
        self._roundtripl_unclosed_short = RoundTripGroup()
        if trade_items is not None:
            if isinstance(trade_items,list):
                for each in trade_items:
                    self.trade(each)

    @staticmethod
    def _df_to_trade_items_list(df):
        return [TradeItem(pos=float(row['transactions']),pos_price=row['transaction_price'],dt=idx) for idx,row in df.iterrows()]

    def hist(self,values,bins = None,xlabel=None,ylabel=None,show=True):
        plt.hist(values, bins=bins, color="g", histtype="bar", rwidth=1, alpha=0.6)
        if xlabel is not None:
            plt.xlabel(xlabel)
        if ylabel is not None:
            plt.ylabel(ylabel)
        if show:
            plt.show()


    def trade(self,trade_item = None):
        if trade_item is None or not isinstance(trade_item,TradeItem):
            return
        trade_item_2 = trade_item
        _roundtripl_unclosed = None
        if isinstance(trade_item_2,SellItem):
            _roundtripl_unclosed = self._roundtripl_unclosed_long
        if isinstance(trade_item_2,BuyItem):
            _roundtripl_unclosed = self._roundtripl_unclosed_short
        if _roundtripl_unclosed is not None:
            if len(_roundtripl_unclosed)>0:
                pop_idx = []
                for i in range(len(_roundtripl_unclosed)):
                    each = _roundtripl_unclosed[i]
                    assert isinstance(each,Signal)
                    ret_trade_item = each.trade(trade_item=trade_item_2)
                    trade_item_2 = None
                    if isinstance(ret_trade_item, Signal):
                        ret_trade_item = [ret_trade_item]

                    if isinstance(ret_trade_item,list):
                        for x in ret_trade_item:
                            if isinstance(x,Signal):
                                if x.is_closed:
                                    self._roundtrip_closed.append(x)
                                    pop_idx.append(i)
                                elif x.is_open:
                                    _roundtripl_unclosed[i] = x
                            elif isinstance(x,TradeItem):
                                trade_item_2 = x
                pop_idx.reverse()
                for i in pop_idx:
                    _roundtripl_unclosed.pop(i)
        if trade_item_2 is not None and not trade_item_2.is_empty:
            if isinstance(trade_item_2,SellItem):
                self._roundtripl_unclosed_short.append(Signal(trade_item=trade_item_2))
            if isinstance(trade_item_2,BuyItem):
                self._roundtripl_unclosed_long.append(Signal(trade_item=trade_item_2))

    def close_all(self,price,dt):
        for _roundtripl_unclosed in [self._roundtripl_unclosed_long,self._roundtripl_unclosed_short]:
            trade_items = [TradeItem(-each.remaining_pos,price,dt) for each in _roundtripl_unclosed if isinstance(each,Signal) ]
            for each in trade_items:
                self.trade(each)

    def agg_by_round_trip(self):
        long_group, short_group = self._roundtrip_closed.group_by_long_short()
        ret = []
        for each in [long_group,short_group]:
            if len(each)>0:
                each_ret = RoundTripGroup._agg_by_same_round_trip(each)
                if isinstance(each_ret,RoundTripGroup):
                    ret.append(each_ret)
                elif isinstance(each_ret,list):
                    ret.extend(each_ret)
        return ret
