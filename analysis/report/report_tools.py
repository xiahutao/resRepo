import os
import pandas
import numpy
from data_engine.data_factory import DataFactory
import data_engine.global_variable as global_variable
from analysis.sector_analysis import SectorAnalysis
from analysis.analysis import Analysis
from reportlab.platypus import Table, SimpleDocTemplate, Paragraph,NextPageTemplate,PageBreak,PageBegin
from reportlab.lib.pagesizes import letter
from data_engine.instrument.future import Future
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph,Spacer,Image,Table
from reportlab.lib.units import cm
import datetime

from common.os_func import check_fold

from analysis.report.graphs import Graphs

class report_tools:
    '''
        summary_report产生汇总报告，包括各板块的'sharpe_ratio', 'max_dd', 'annual_return', 'annual_volatility', 'sortino_ratio',
             'calmar_ratio', ；  Cumulative_returns曲线；annual_returns曲线；
        sharpelist_report产生各品种指标的列表，指标包括'sharpe_ratio', 'max_dd', 'annual_return', 'annual_volatility', 'sortino_ratio',
             'calmar_ratio',; 提供汇总至sector, all层级的指标列表
    '''
    @staticmethod
    def summary_report(strategy_name,
                          sector_list,name_list,daily_returns_list,
                          pdf_filename,pdf_path=None):
        styles = getSampleStyleSheet()

        sector_analysis = SectorAnalysis(sector='all', daily_returns_list=daily_returns_list, name_list=name_list)
        content = list()
        content.append(Graphs.draw_title('Summary_' + strategy_name + '(%s)' % datetime.datetime.now().strftime('%Y%m%d')))
        # content.append(Paragraph(title, styles['Heading1']))
        content.append(Spacer(0, 0.5 * cm))
        content = content + sector_analysis.gen_report_content()

        for sector in ['Grains', 'Chem', 'BaseMetal', 'Bulks', 'PreciousMetal', 'Bonds', 'Equity']:
            daily_returns_list_sector = []
            name_list_sector = []
            for x in zip(sector_list, name_list, daily_returns_list):
                if x[0] == sector:
                    daily_returns_list_sector.append(x[2])
                    name_list_sector.append(x[1])
            if len(daily_returns_list_sector) > 0:
                sector_analysis = SectorAnalysis(sector=sector, daily_returns_list=daily_returns_list_sector,
                                                 name_list=name_list_sector)
                content.append(PageBreak())
                content = content + sector_analysis.gen_report_content()

        filename = pdf_filename
        if pdf_path is not None:
            check_fold(pdf_path)
            filename= os.path.join(pdf_path,pdf_filename)
        doc = SimpleDocTemplate(filename, pagesize=letter)
        doc.build(content)

    @staticmethod
    def sharpelist_report(strategy_name,
                          sector_list,name_list,daily_returns_list,
                          pdf_filename,pdf_path=None):
        styles = getSampleStyleSheet()
        content = list()
        content.append(Graphs.draw_title('Sharpelist: ' + strategy_name + '(%s)' % datetime.datetime.now().strftime('%Y%m%d')))
        content.append(Spacer(0, 0.5 * cm))

        sector = 'all'
        index_dict_list = []
        sector_analysis = SectorAnalysis(sector=sector, daily_returns_list=daily_returns_list, name_list=name_list)
        index_dict = sector_analysis.get_index_dict(format=True)
        index_dict['sector'] = sector
        index_dict_list.append(pandas.Series(index_dict))
        index_dict_df = pandas.concat(index_dict_list, axis=1).T.sort_values(['sharpe'], ascending=False)
        index_dict_df = index_dict_df[
            ['sector', 'sharpe', 'max_dd', 'annual_ret', 'annual_vol', 'sortino',
             'calmar']]

        data = [tuple(index_dict_df.columns)] + [tuple(x.to_dict().values()) for idx, x in index_dict_df.iterrows()]
        content.append(Paragraph('ALL: ', styles['Heading1']))
        content.append(Graphs.draw_table(*data, ALIGN='LEFT', VALIGN='RIGHT',
                                         col_width=[80] + [60] * (len(index_dict_df.columns) - 1)))
        content.append(Spacer(0, 0.5 * cm))

        index_dict_list = []
        for sector in ['Grains', 'Chem', 'BaseMetal', 'Bulks', 'PreciousMetal', 'Bonds', 'Equity']:
            daily_returns_list_sector = []
            name_list_sector = []
            for x in zip(sector_list, name_list, daily_returns_list):
                if x[0] == sector:
                    daily_returns_list_sector.append(x[2])
                    name_list_sector.append(x[1])
            if len(daily_returns_list_sector) > 0:
                sector_analysis = SectorAnalysis(sector=sector, daily_returns_list=daily_returns_list_sector,
                                                 name_list=name_list_sector)
                index_dict = sector_analysis.get_index_dict(format=True)
                index_dict['sector'] = sector
                index_dict_list.append(pandas.Series(index_dict))
        index_dict_df = pandas.concat(index_dict_list, axis=1).T.sort_values(['sharpe'], ascending=False)
        index_dict_df = index_dict_df[
            ['sector', 'sharpe', 'max_dd', 'annual_ret', 'annual_vol', 'sortino',
             'calmar']]

        data = [tuple(index_dict_df.columns)] + [tuple(x.to_dict().values()) for idx, x in index_dict_df.iterrows()]
        content.append(Paragraph('SECTOR: ', styles['Heading1']))
        content.append(Graphs.draw_table(*data, ALIGN='LEFT', VALIGN='RIGHT',
                                         col_width=[80] + [60] * (len(index_dict_df.columns) - 1)))
        content.append(Spacer(0, 0.5 * cm))

        index_dict_list = []
        for x in zip(sector_list, name_list, daily_returns_list):
            sector = x[0]
            name = x[1]
            daily_returns = x[2]
            analysis_obj = Analysis(daily_returns=daily_returns)
            index_dict = analysis_obj.get_index_dict(format=True)
            index_dict['symbol'] = name
            index_dict['sector'] = sector
            index_dict_list.append(pandas.Series(index_dict))
        index_dict_df = pandas.concat(index_dict_list, axis=1).T.sort_values(['sharpe'], ascending=False)
        index_dict_df = index_dict_df[
            ['symbol', 'sector', 'sharpe', 'max_dd', 'annual_ret', 'annual_vol', 'sortino',
             'calmar']]

        data = [tuple(index_dict_df.columns)] + [tuple(x.to_dict().values()) for idx, x in index_dict_df.iterrows()]
        content.append(Paragraph('SYMBOL: ', styles['Heading1']))
        content.append(Graphs.draw_table(*data, ALIGN='LEFT', VALIGN='RIGHT',
                                         col_width=[60] + [60] * (len(index_dict_df.columns) - 1)))
        content.append(Spacer(0, 0.5 * cm))

        filename = pdf_filename
        if pdf_path is not None:
            check_fold(pdf_path)
            filename= os.path.join(pdf_path,pdf_filename)
        index_dict_df.to_csv(filename.replace('.pdf','.csv'))
        doc = SimpleDocTemplate(filename, pagesize=letter)
        doc.build(content)


