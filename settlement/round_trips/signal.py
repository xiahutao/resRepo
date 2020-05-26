from settlement.round_trips.trade_item import *
from settlement.round_trips.round_trip import RoundTrip
class Signal(RoundTrip):
    def __init__(self,trade_item):
        RoundTrip.__init__(self)
        self.set_open(trade_item)

    def __new__(cls, trade_item = None):
        if trade_item is not None and isinstance(trade_item,TradeItem):
            if trade_item.is_buy and not issubclass(cls,LongSignal):
                return  LongSignal.__new__(LongSignal,trade_item)
            if trade_item.is_sell and not issubclass(cls,ShortSignal):
                return  ShortSignal.__new__(ShortSignal,trade_item)
            if trade_item.is_empty and not issubclass(cls,EmptySignal):
                return EmptySignal.__new__(EmptySignal,trade_item)
        return object.__new__(cls)

    def trade(self,trade_item):
        if isinstance(trade_item,BuyItem):
            return self.buy(trade_item)
        elif isinstance(trade_item,SellItem):
            return self.sell(trade_item)
        return self

    def buy(self,trade_item):
        assert isinstance(trade_item,BuyItem)
        return Signal(trade_item)

    def sell(self,trade_item):
        assert isinstance(trade_item,SellItem)
        return Signal(trade_item)

def in_same_round_trip(signal_0,signal):
    if (isinstance(signal_0,Signal) or isinstance(signal_0,RoundTripGroup)) \
        and  (isinstance(signal,Signal) or isinstance(signal,RoundTripGroup)):
        pass
    else:
        return False
    if signal_0.open_dt is None or signal_0.open_dt is None:
        return True
    if signal.open_dt is None or signal.open_dt is None:
        return True
    if signal.open_dt is not None \
            and signal.open_dt >= signal_0.open_dt \
            and signal.open_dt <= signal_0.close_dt:
        return True
    if signal.close_dt is not None \
            and signal.close_dt >= signal_0.open_dt \
            and signal.close_dt <= signal_0.close_dt:
        return True
    return False

class EmptySignal(Signal):
    pass

class LongSignal(Signal):

    def buy(self,trade_item):
        assert isinstance(trade_item,BuyItem)
        return [self,Signal(trade_item)]

    def sell(self,trade_item):
        assert isinstance(trade_item,SellItem)
        if trade_item.pos == -self.remaining_pos:
            return self.set_close(trade_item=trade_item)
        elif trade_item.pos < -self.remaining_pos:
            return [
                trade_item.clone(new_pos=trade_item.pos + self.remaining_pos),
                self.set_close(trade_item=trade_item.clone(new_pos=-self.remaining_pos))
            ]
        else:
            return [
                trade_item.clone(new_pos=trade_item.pos + self.remaining_pos)
                , self.set_close(trade_item=trade_item, set_open_pos=True)
            ]

class ShortSignal(Signal):
    def sell(self,trade_item):
        assert isinstance(trade_item,SellItem)
        return [self,Signal(trade_item)]

    def buy(self,trade_item):
        assert isinstance(trade_item,BuyItem)
        if trade_item.pos == -self.remaining_pos:
            return self.set_close(trade_item=trade_item)
        elif trade_item.pos < -self.remaining_pos:
            return [
                trade_item.clone(new_pos=trade_item.pos + self.remaining_pos)
                , self.set_close(trade_item=trade_item, set_open_pos=True)
            ]
        else:
            return [
                trade_item.clone(new_pos=trade_item.pos + self.remaining_pos),
                self.set_close(trade_item=trade_item.clone(new_pos=-self.remaining_pos))
            ]
