#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/15 10:25
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm


import os
import pandas
import numpy
import data_engine.global_variable as global_variable
from data_engine.data_factory import DataFactory


class mongo_object(object):
    def __init__(self):
        self._mongo = DataFactory.get_mongo_client()

    def __del__(self):
        self._mongo.close()

    @property
    def mongo_client(self):
        return self._mongo