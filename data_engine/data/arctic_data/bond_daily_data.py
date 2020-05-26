# -*- coding: utf-8 -*-

from arctic import Arctic

from data_engine.data.bond_data import BondData
from data_engine.setting import FREQ_1D,BOND_DAILY_LIB
import data_engine.setting as Setting


class BondDailyData(BondData):
    """
    债券日线数据接口，从mongo读取
    """
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        BondData.__init__(self, freq=FREQ_1D,price_type=price_type,**kwargs)

    def _get_data(self, symbol, start_date, end_date, **kwargs):
        if isinstance(symbol,str):
            df = self.arc_lib.read(symbol)
            return df[start_date: end_date]
        elif isinstance(symbol,list):
            df_dict = self.arc_lib.read(symbol)
            return df_dict

    def _get_arclib_str(self):
        return BOND_DAILY_LIB
