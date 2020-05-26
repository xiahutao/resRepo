#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/12/18 9:09
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import time
import data_engine.setting as setting

def runing_time(func):
    def clock_time(*args, **kwargs):
        if setting.logging_level in [setting.logging.DEBUG]:
            print('=================',func.__name__)
        t1 = time.clock()
        ret = func(*args, **kwargs)
        t2 = time.clock()
        if setting.logging_level in [setting.logging.DEBUG]:
            print('=================',func.__name__,'%.6fs' % (t2-t1))
        return ret
    return clock_time