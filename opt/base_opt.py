#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/2/24 15:13
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import optuna
import pandas
import os
from common.cache import cache
from data_engine.data_factory import DataFactory

class BaseOpt:
    def __init__(self,look_back_start,look_back_end
                 ,n_trials=100,n_jobs=1
                 ,cache_namespace='cache_namespace',load_cache=False):
        self._look_back_start = look_back_start
        self._look_back_end = look_back_end

        self._n_trials = n_trials
        self._n_jobs = n_jobs
        self._study = None

        self._cache_namespace = cache_namespace
        if load_cache:
            cache.load(namespace=self._cache_namespace)
    def __del__(self):
        DataFactory().clear_data()

    def objective(self,trial):
        return 0

    def dump_result(self):
        cache.dump(namespace=self._cache_namespace)

    def load_result(self):
        cache.load(namespace=self._cache_namespace)

    def cache_result(self,cache_object,*args):
        assert len(args)>0
        cache.set_object(tuple(args),cache_object,namespace=self._cache_namespace)

    def get_cache(self,*args):
        assert len(args)>0
        return cache.get_object(tuple(args),namespace=self._cache_namespace)


    def opt_dump(self,best_arg_path,result_filename,direction='maximize'):
        best_args,trials_dataframe = self.opt(direction=direction)
        pandas.Series(best_args).to_csv(os.path.join(best_arg_path, result_filename))
        trials_dataframe.sort_values('value',ascending=False).to_csv(os.path.join(best_arg_path, 'trials_dataframe_' + result_filename))
        return best_args,trials_dataframe

    def opt(self,direction='maximize'):
        '''
        :param direction: minimize or maximize
        :return:
        '''
        study = optuna.create_study(direction=direction)
        study.optimize(self.objective, n_trials=self._n_trials,n_jobs=self._n_jobs)
        best_args = study.best_params
        best_args['look_back_start'] = self._look_back_start
        best_args['look_back_end'] = self._look_back_end
        best_args['direction'] = study.direction
        best_args['best_value'] = study.best_value

        self._study = study
        return best_args, study.trials_dataframe()

