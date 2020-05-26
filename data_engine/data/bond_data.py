from data_engine.data.market_data import MarketData
from data_engine.setting import ASSETTYPE_BOND
import data_engine.setting as Setting

class BondData(MarketData):
    def __init__(self, freq,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        MarketData.__init__(self, freq=freq, asset_type=ASSETTYPE_BOND,price_type=price_type,**kwargs)
