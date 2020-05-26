# -*- coding: utf-8 -*-

from data_engine.data.market_data import MarketData
from data_engine.setting import ASSETTYPE_STOCK,PRICE_TYPE_POST,PRICE_TYPE_PRE,PRICE_TYPE_UN


class StockData(MarketData):
    def __init__(self,freq,price_type=PRICE_TYPE_UN,**kwargs):
        MarketData.__init__(self,freq=freq,asset_type=ASSETTYPE_STOCK,price_type=price_type,**kwargs)

    def _get_arclib_str(self):
        return None

    @staticmethod
    def _format_data(datadf):
        if 'settle' not in datadf.columns:
            datadf['settle'] = datadf['close']
        for nextcol, col in {'next_close':'close',
                             'next_open': 'open',
                             'next_high': 'high',
                             'next_low': 'low',
                             'next_settle': 'settle',
                             }.items():
            if nextcol not in datadf.columns:
                datadf[nextcol] = datadf[col].shift(-1)
        for nextcol, col in {'last_close':'close',
                             'last_open': 'open',
                             'last_high': 'high',
                             'last_low': 'low',
                             'last_settle': 'settle',
                             }.items():
            if nextcol not in datadf.columns:
                datadf[nextcol] = datadf[col].shift(1)
        return datadf