import numpy
import copy
class TradeItem(object):
    def __init__(self,pos=0,pos_price=None,dt=None):
        self._pos = pos
        self._pos_price = pos_price
        self._dt = dt

    def __new__(cls,pos=0,pos_price=None,dt=None):
        if not issubclass(cls,BuyItem) and pos > 0:
            return BuyItem.__new__(BuyItem,pos,pos_price,dt)
        if not issubclass(cls,SellItem) and pos < 0:
            return SellItem.__new__(SellItem,pos,pos_price,dt)
        return object.__new__(cls)

    def __repr__(self):
        return '%s: %s, %s' %(self.dt,self._pos,self.pos_price)

    def set_pos(self,new_pos):
        if new_pos is not None:
            self._pos = new_pos
        return self

    def trading_money(self,contract_size=1):
        return self._pos * self._pos_price * contract_size

    @property
    def pos_price(self):
        return self._pos_price
    @property
    def dt(self):
        return self._dt
    @property
    def pos(self):
        return round(numpy.abs(self._pos),5) * numpy.sign(self._pos)
    @property
    def is_buy(self):
        return self._pos is not None and self.pos > 0
    @property
    def is_sell(self):
        return self._pos is not None and self.pos < 0
    @property
    def is_empty(self):
        return self._pos is None or self.pos == 0

    def clone(self,new_pos = None):
        return TradeItem(pos=new_pos,pos_price=self._pos_price,dt=self._dt)# copy.copy(self).set_pos(new_pos=new_pos)

class BuyItem(TradeItem):
    pass
class SellItem(TradeItem):
    pass
