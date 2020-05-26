import datetime
import numpy
from settlement.round_trips.trade_item import *
class RoundTrip(object):
    def __init__(self):
        self._open = None
        self._close = None

    def __repr__(self):
        return '%s | %s' %(str(self._open),str(self._close))

    @property
    def remaining_pos(self):
        if self._open is None:
            return 0
        elif self._close is None:
            return self._open.pos
        else:
            return  max(0,abs(self._open.pos) - abs(self._close.pos)) * numpy.sign(self._open.pos)

    @property
    def open_dt(self):
        if self._open is not None and isinstance(self._open,TradeItem):
            return self._open.dt
        return None

    def open_pos(self):
        if self._open is not None and isinstance(self._open,TradeItem):
            return self._open.pos
        return None

    @property
    def close_dt(self):
        if self._close is not None and isinstance(self._close,TradeItem):
            return self._close.dt
        return None

    @property
    def hold_period(self):
        dt1 = self.open_dt
        dt2 = self.close_dt
        if dt1 is None or dt2 is None:
            return None
        return dt2 - dt1

    @property
    def open_trade_item(self):
        return self._open

    @property
    def close_trade_item(self):
        return self._close

    def set_open(self,trade_item):
        if isinstance(trade_item,TradeItem):
            self._open = trade_item
        return self

    def set_close(self,trade_item,set_open_pos=False):
        if isinstance(trade_item,TradeItem):
            self._close = trade_item
            if set_open_pos:
                self._open.set_pos(-self._close.pos)
        return self

    @property
    def is_open(self):
        return self._open is not None and not self._open.is_empty

    @property
    def is_closed(self):
        return self._open is not None and self._close is not None and self.remaining_pos == 0

    def pnl(self,contract_size=1,current_price=None):
        if self._open is None or not isinstance(self._open,TradeItem):
            return None
        if self._close is not None and isinstance(self._close,TradeItem):
            return -self._open.trading_money(contract_size=contract_size) - self._close.trading_money(contract_size=contract_size)
        elif current_price is not None:
            return -self._open.trading_money(contract_size = contract_size) - TradeItem(pos = -self._open.pos, pos_price=current_price).trading_money(contract_size=contract_size)
        return None

    def hold_return(self,current_price=None,margin_ratio = 1.0):
        if self._open is None or not isinstance(self._open,TradeItem):
            return None
        if self._close is not None and isinstance(self._close,TradeItem):
            return (-self._open.trading_money() - self._close.trading_money()) / abs(self._open.trading_money() * margin_ratio)
        elif current_price is not None:
            return (-self._open.trading_money() - TradeItem(pos = -self._open.pos, pos_price=current_price).trading_money()) / abs(self._open.trading_money() * margin_ratio)
        return None


    def daily_return(self,current_price=None,margin_ratio = 1.0,days_year=365):
        hr = self.hold_return(current_price=current_price,margin_ratio=margin_ratio)
        if hr is None:
            return None
        hd_period = self.hold_period
        if hd_period is None:
            return None
        return hr * (datetime.timedelta(days=days_year) / hd_period)
