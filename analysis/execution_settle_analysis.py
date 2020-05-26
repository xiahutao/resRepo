#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/1/3 8:52
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

from execution.execution import Execution_ex
from execution.execution_v2 import Execution_ex as Execution_ex_v2
from settlement.settlement import Settlement_ex
from analysis.analysis import Analysis_ex

def execution_settle_analysis(signal_dataframe,config):
    (success, positions_dataframe) = Execution_ex(config=config).exec_trading(signal_dataframe=signal_dataframe)
    if success:
        settlement_obj = Settlement_ex(config=config)
        settlement_obj.settle(positions_dataframe=positions_dataframe)
        analysis_obj = Analysis_ex(config=config, settlement_obj=settlement_obj)
        return analysis_obj

    return None


def execution_settle_analysis_v2(signal_dataframe,config):
    (success, positions_dataframe) = Execution_ex_v2(config=config).exec_trading(signal_dataframe=signal_dataframe)
    if success:
        settlement_obj = Settlement_ex(config=config)
        settlement_obj.settle(positions_dataframe=positions_dataframe)
        analysis_obj = Analysis_ex(config=config, settlement_obj=settlement_obj)
        return analysis_obj

    return None