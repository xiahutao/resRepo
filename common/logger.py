#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/9 14:31
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import logging
import datetime
from common.singleton import Singleton
from data_engine.data_factory import DataFactory

class logger(object,metaclass=Singleton):
    def __init__(self,name=None):
        self._logger = logging.getLogger(name=name)
        self._formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    @staticmethod
    def insert_record_to_updateLog(machine_IP, task_name,**kwargs):
        try:
            mongo=DataFactory.get_mongo_client()
            db = mongo.get_database('updateLog')
            cl = db.get_collection('task_log')
            cl.insert({'update_time': datetime.datetime.now(), 'task_name': task_name, 'machine': machine_IP,**kwargs})
        except:
            pass

    @staticmethod
    def get_logger(filename):
        return logger() \
            .with_file_log(filename) \
            .with_scream_log() \
            .setLevel()

    def setLevel(self,level=logging.DEBUG):
        self._logger.setLevel(level)
        return self

    def with_file_log(self,filename,level=logging.DEBUG):
        fh = logging.FileHandler(filename)
        fh.setLevel(level)
        fh.setFormatter(self._formatter)
        self._logger.addHandler(fh)
        return self

    def with_scream_log(self,level=logging.DEBUG):
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(self._formatter)
        self._logger.addHandler(ch)
        return self

    def debug(self,msg, *args, **kwargs):
        self._logger.debug(msg)
        if len(args)>0:
            for x in args:
                self._logger.debug(x)
        if len(kwargs)>0:
            for x,y in kwargs.items():
                self._logger.debug(y)
    def info(self,msg, *args, **kwargs):
        self._logger.debug(msg)
        if len(args)>0:
            for x in args:
                self._logger.info(x)
        if len(kwargs)>0:
            for x,y in kwargs.items():
                self._logger.info(y)
    def warn(self,msg, *args, **kwargs):
        self._logger.debug(msg)
        if len(args)>0:
            for x in args:
                self._logger.warn(x)
        if len(kwargs)>0:
            for x,y in kwargs.items():
                self._logger.warn(y)
    def error(self,msg, *args, **kwargs):
        self._logger.error(msg)
        if len(args)>0:
            for x in args:
                self._logger.error(x)
        if len(kwargs)>0:
            for x,y in kwargs.items():
                self._logger.error(y)
    def critical(self,msg, *args, **kwargs):
        self._logger.critical(msg)
        if len(args)>0:
            for x in args:
                self._logger.critical(x)
        if len(kwargs)>0:
            for x,y in kwargs.items():
                self._logger.critical(y)
if __name__ == '__main__':
    log = logger(name='pair').setLevel().with_scream_log().with_file_log(filename=r'e:\\pair.log')

    # 开始打日志
    log.debug("debug message")
    log.debug("debug message","debug message","debug message",1000,[1,2])
    log.info("info message")
    log.warn("warn message")
    log.error("error message")
    log.critical("critical message")