# -*- coding: utf-8 -*-

import os
import logging
import pyodbc

import numpy as np
import pandas as pd

from data_engine.data.file_data.file_data import FileData
from data_engine.setting import FREQ_TICK
import data_engine.setting  as Setting

_logger = logging.getLogger(__name__)

class FileTickData(FileData):
    '''
        分钟数据接口，从本地文件加载
    '''
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        FileData.__init__(self,freq=FREQ_TICK,price_type=price_type,**kwargs)
        self._folder = Setting.TICK_FILE_SOURCE