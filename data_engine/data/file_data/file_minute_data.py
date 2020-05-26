# -*- coding: utf-8 -*-

import os
import logging
import pyodbc

import numpy as np
import pandas as pd

from data_engine.data.file_data.file_data import FileData
from data_engine.setting import FREQ_1M,FREQ_5M
import data_engine.setting  as Setting

_logger = logging.getLogger(__name__)

class FileMinuteData(FileData):
    '''
        分钟数据接口，从本地文件加载
    '''
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        FileData.__init__(self,freq=FREQ_1M,price_type=price_type,**kwargs)
        self._folder = Setting.MINUTE_FILE_SOURCE


class File5MinuteData(FileData):
    '''
        5分钟数据接口，从本地文件加载
    '''
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        FileData.__init__(self,freq=FREQ_5M,price_type=price_type,**kwargs)
        self._folder = Setting.MINUTE_FILE_SOURCE_5M