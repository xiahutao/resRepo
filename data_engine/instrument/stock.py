#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/8 16:37
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import data_engine.global_variable as global_variable
from data_engine.instrument.instrument import Instrument

class Stock(Instrument):
    def __init__(self,symbol):
        Instrument.__init__(self,symbol=symbol,asset_type=global_variable.ASSETTYPE_STOCK)
