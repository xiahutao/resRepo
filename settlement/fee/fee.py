#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/12/5 15:46
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import numpy
class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Fee(object,metaclass=Singleton):
    def __init__(self):
        pass
    def calc_fee(self, product_id, volumn, price, contract_size, is_open=True):
        return 0

    def calc_fee_ex(self, transaction_df):
        return 0