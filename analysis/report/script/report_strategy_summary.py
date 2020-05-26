import os
import pandas
import numpy
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, A4
from data_engine.data_factory import DataFactory
import data_engine.global_variable as global_variable
from analysis.sector_analysis import SectorAnalysis
from reportlab.platypus import Table, SimpleDocTemplate, Paragraph,NextPageTemplate,PageBreak,PageBegin
from reportlab.lib.pagesizes import letter
from data_engine.instrument.future import Future
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph,Spacer,Image,Table
from reportlab.lib.units import cm
import datetime
from analysis.report.report_tools import report_tools
from common.os_func import get_file_list

from common.os_func import check_fold
from analysis.report.graphs import Graphs

def report(strategy_name,format_folder_func,path,filename = 'daily_returns.csv',pdf_path=r'e:/report'):
    '''
    :param strategy_name:  策略的名称
    :param format_folder_func:  用于在提取daily_returns时候，通过对应文件夹的名称提取品种信息
    :param path: 回测文件路径
    :param filename: daily_returns.csv 或 daily_return_by_init_aum.csv
    :param pdf_path: 报告保存路径
    :return:
    '''
    sector_list, name_list, daily_returns_list = get_file_list(path=path,format_folder_func=format_folder_func,filename=filename)
    report_tools.summary_report(strategy_name=strategy_name
                                   ,sector_list=sector_list, name_list=name_list, daily_returns_list=daily_returns_list
                                   ,pdf_filename='summary_' + strategy_name + '.pdf',pdf_path=pdf_path)
if __name__ == '__main__':

    DataFactory.config('password',DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)

    # format_folder_func = lambda x : x.replace('_5m','')
    # strategy_name = 'resFuturesTrendIntraday'
    # path = r'E:\results_20200214\results_20200214'
    # filename = 'daily_returns.csv'
    # report(strategy_name=strategy_name,format_folder_func=format_folder_func,path=path,filename=filename)

    format_folder_func = lambda x: x.split('_')[0]
    strategy_name = 'resBaseMomentum'
    path = r'E:/BaseMomentum_args_opt'
    filename = 'daily_return_by_init_aum.csv'
    report(strategy_name=strategy_name, format_folder_func=format_folder_func, path=path, filename=filename, pdf_path=path)
    #
    # format_folder_func = lambda x : x.replace('resRepo_compair_all_','').replace('_5M_exec_1','').split('_')[0]
    # # format_folder_func = lambda x : x.split('_')[0]
    # strategy_name = 'resFuturesStarb'
    # path = r'E:\PairStrategy'
    # # path = r'E:\PairStrategy_args_opt'
    # filename = 'daily_return_by_init_aum.csv'
    # report(strategy_name=strategy_name,format_folder_func=format_folder_func,path=path,filename=filename)
    # # report(strategy_name=strategy_name,format_folder_func=format_folder_func,path=path,filename=filename
    # #        ,pdf_path=path)