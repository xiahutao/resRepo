#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/6 16:20
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
from common.file_saver import file_saver
from analysis.analysis import Analysis_ex
from analysis.execution_settle_analysis import execution_settle_analysis
from multiprocessing import Pool,cpu_count

class Portfolio_analysis(object):
    def __init__(self,config_list,signal_dataframe_list):
        self._config_list = config_list
        self._signal_dataframe_list = signal_dataframe_list

    def execution_settle_analysis(self):
        pool = Pool(cpu_count() - 1)
        self._analysis_list = pool.map(execution_settle_analysis,zip(self._signal_dataframe_list,self._config_list))

        [each.save_result() for each in self._analysis_list]
        [each.plot_cumsum_pnl(show=False) for each in self._analysis_list]


if __name__ == '__main__':
    pass
