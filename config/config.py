#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/2 10:23
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import os
import yaml
import csv
import numpy
import data_engine.global_variable as  global_variable
from common.os_func import check_fold

class Config_base(object):
    def __init__(self,config_path=None):
        self._config_path = config_path
        if self._config_path:
            check_fold(self._config_path)

        self._config_object = None
        self._config_object_list = None

    def __repr__(self):
        if self._config_object is not None:
            return yaml.safe_dump(self._config_object)
        return ''

    def _get_file_path(self,config_file):
        if self._config_path is not None:
            return os.path.join(self._config_path,config_file)
        else:
            return config_file

    def dump(self,config_file):
        f = open(self._get_file_path(config_file=config_file), 'w', encoding='utf-8')
        yaml.safe_dump(self._config_object, f)
        f.close()
        return self

    def dump_all(self,config_file):
        f = open(self._get_file_path(config_file=config_file),'w',encoding='utf-8')
        yaml.safe_dump_all(self._config_object_list, f)
        f.close()
        return self

    def load(self,config_file):
        f = open(self._get_file_path(config_file=config_file),'r',encoding='utf-8')
        self._config_object = yaml.safe_load(f)
        f.close()
        return self

    def get_config_item(self,key):
        if not key in self._config_object:
            return None
        return self._config_object[key]

    def set_config_item(self,key,value):
        if type(value) == numpy.float64:
            value = float(value)
        self._config_object[key] = value

    def load_all(self,config_file):
        self._config_object_list = []
        f = open(self._get_file_path(config_file=config_file),'r',encoding='utf-8')
        data_generator = yaml.safe_load_all(f)
        for data in data_generator:
            self._config_object_list.append(data)
        f.close()
        return self

class Config_trading(Config_base):
    def __init__(self,config_path=None):
        Config_base.__init__(self,config_path=config_path)
        self._config_object = {}



    def get_config_item(self,key):
        if key in self._config_object:
            return self._config_object[key]
        return None

    def dump_csv(self,config_file, append=True,with_key=True):
        if append:
            f = open(self._get_file_path(config_file=config_file), 'a', encoding='utf-8')
        else:
            f = open(self._get_file_path(config_file=config_file), 'w', encoding='utf-8')
        if with_key:
            f.write(','.join([ '{}={}'.format(x,y)  for x, y in self._config_object.items()]))
        else:
            f.write(','.join([ '{}'.format(y)  for x, y in self._config_object.items()]))

        f.write('\n')
        f.close()
        return self

class Config_back_test(Config_base):

    def gen_config_struct(self):
        self._config_object={
            'strategy':{'id':None,'capital':100000,'strategy_type':None},
            'data':{'freq':global_variable.FREQ_1M},
            'result':{'result_folder':None},
            'execution':{'exec_lag':1},
            'settle':{}
        }

    def __get__(self, instance, owner):
        if self.strategy_id is None:
            print('strategy_id is None!!')
        return self

    @property
    def strategy_id(self):
        config = self.strategy_config
        if 'id' in config:
            return '_'.join([self.strategy_type,str(config['id'])])
        return None

    @strategy_id.setter
    def strategy_id(self,value):
        self._init_config_object(subkey='strategy')
        self._config_object['strategy']['id'] = value

    @property
    def result_folder(self):
        config = self.result_config
        if 'result_folder' in config:
            return config['result_folder']
        return None

    @property
    def strategy_type(self):
        config = self.strategy_config
        if 'strategy_type' in config:
            return config['strategy_type']
        return None
    # @property
    # def strategy_id(self):
    #     config = self.strategy_config
    #     config_data = self.data_config
    #     config_execution = self.execution_config
    #     return '_'.join([config['strategy_type']
    #                         ,str(config['id'])
    #                         ,str(config_data['freq'])
    #                         ,str(config_execution['exec_lag'])
    #                      ])

    @property
    def strategy_config(self):
        self._init_config_object(subkey='strategy')
        if self._config_object is not None:
            return self._config_object['strategy']
        return None

    def get_strategy_config(self,item):
        config = self.strategy_config
        if config is not None and item in config:
            return config[item]
        return None

    @property
    def execution_config(self):
        self._init_config_object(subkey='execution')
        if self._config_object is not None:
            return self._config_object['execution']
        return None

    def get_execution_config(self,item):
        config = self.execution_config
        if config is not None and item in config:
            return config[item]
        return None

    def get_execution_freq(self):
        freq = self.get_data_config('freq')
        if 'freq' in self.execution_config:
            freq = self.get_execution_config('freq')
        return freq

    @property
    def settle_config(self):
        self._init_config_object(subkey='settle')
        if self._config_object is not None:
            return self._config_object['settle']
        return None

    def get_settle_config(self,item):
        config = self.settle_config
        if config is not None and item in config:
            return config[item]
        return None

    @property
    def data_config(self):
        self._init_config_object(subkey='data')
        if self._config_object is not None:
            return self._config_object['data']
        return None

    def get_data_config(self,item):
        config = self.data_config
        if config is not None and item in config:
            return config[item]
        return None

    @property
    def result_config(self):
        self._init_config_object(subkey='result')
        if self._config_object is not None:
            return self._config_object['result']
        return None

    def get_result_config(self,item):
        config = self.result_config
        if config is not None and item in config:
            return config[item]
        return None

    def _init_config_object(self,subkey=None):
        if self._config_object is None:
            self._config_object = {}
        if subkey is not None:
            if subkey not in self._config_object:
                self._config_object[subkey] = {}

    def set_result_folder(self,result_folder):
        self._init_config_object(subkey='result')
        self._config_object['result']['result_folder'] = result_folder
        check_fold(result_folder)

if __name__ == '__main__':
    config = Config_back_test()
    config.load('sample_single.yaml')
    print(config._config_object)
    print(config.execution_config)
    print(config.settle_config)
    print(config.data_config)
    print(config.strategy_config)

    config2 = Config_back_test()
    config2.gen_config_struct()
    config2.dump(config_file='config_sample.yaml')