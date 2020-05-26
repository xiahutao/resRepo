#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/2/27 8:43
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import os
import threading
import pickle
from common.singleton import Singleton
import data_engine.setting as setting
from common.os_func import check_fold
from common.file_saver import file_saver

class cache(object,metaclass=Singleton):
    cache_file_prefix = 'cache_np_'
    def __init__(self):
        self._cache_dict = {}

    @staticmethod
    def cache_path():
        path = setting.TEMP
        if path is None:
            path = os.path.curdir
        return os.path.join(path,'cache')

    @staticmethod
    def dump(namespace=None):
        c = cache()
        path = cache.cache_path()
        check_fold(path)
        if namespace is None:
            for each,data in c._cache_dict.items():
                file_saver().pickle_file(data,os.path.join(path,cache.cache_file_prefix + each + '.pkl'))
        elif namespace in c._cache_dict:
            file_saver().pickle_file(c._cache_dict[namespace],os.path.join(path,cache.cache_file_prefix + namespace + '.pkl'))

    @staticmethod
    def join():
        file_saver().join()

    @staticmethod
    def load(namespace=None):
        c = cache()
        path = cache.cache_path()
        if namespace is not None:
            filename = os.path.join(path,cache.cache_file_prefix + namespace + '.pkl')
            if os.path.exists(filename):
                try:
                    c._cache_dict[namespace] = pickle.load(open(filename,'rb'))
                except EOFError:
                    return False
                return True
        else:
            for x in os.walk(path):
                for filename in x[2]:
                    if cache.cache_file_prefix in filename and '.pkl' in filename:
                        try:
                            c._cache_dict[filename.replace(cache.cache_file_prefix,'').replace('.pkl','')] = pickle.load(open(os.path.join(x[0],filename),'rb'))
                        except EOFError:
                            return False
        return False

    @staticmethod
    def set_object(key, value, namespace='cache'):
        c = cache()
        if namespace not in c._cache_dict:
            c._cache_dict[namespace] = {}
        c._cache_dict[namespace][key] = value

    @staticmethod
    def get_object(key,namespace='cache'):
        c = cache()
        if namespace not in c._cache_dict:
            return None
        if key not in c._cache_dict[namespace]:
            return None
        return c._cache_dict[namespace][key]

    @staticmethod
    def clear_cache(namespace=None):
        c = cache()
        if namespace is None:
            c._cache_dict = {}
        elif namespace in c._cache_dict:
            c._cache_dict[namespace] = {}



    @staticmethod
    def list_namespace():
        c = cache()
        return c._cache_dict.keys()

    @staticmethod
    def list_key(namespace):
        c = cache()
        if namespace is None:
            return None
        elif namespace in c._cache_dict:
            return c._cache_dict[namespace].keys()
        else:
            return None