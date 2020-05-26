import os
import pickle
import pandas
import data_engine.global_variable as global_variable
from analysis.analysis import Analysis
import matplotlib.pyplot as plt
from analysis.report.graphs import Graphs
from io import BytesIO
from svglib.svglib import svg2rlg
from reportlab.platypus import Paragraph,Spacer,Image,Table,NextPageTemplate
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet

class SectorAnalysis(Analysis):
    '''
        通过输入组合（sector）的daily_returns_list数据，产生组合后的收益结果；
        name_list为的daily_returns_list对应的品种名称
        weights为组合汇总时候的的权重，暂时未支持
    '''
    def __init__(self,sector,daily_returns_list,name_list=None,by_mean=True):
        self._sector = sector
        self._daily_returns_list = daily_returns_list
        self._name_list = name_list

        self._analysis_obj = None
        self._daily_returns = SectorAnalysis.process_daily_list(self._daily_returns_list,by_mean=by_mean)
        Analysis.__init__(self,daily_returns=self._daily_returns)
    #
    # def sector_index_series(self):
    #     index_dict = self.get_index_dict()
    #     ret = pandas.Series(index_dict)
    #     ret.name = 'bt_index'
    #     return ret

    def gen_report_content(self):
        styles = getSampleStyleSheet()

        annual_returns = self.annual_returns()
        annual_returns_data = [(x, '%.2f%%' % (y*100) ) for x, y in annual_returns.to_dict().items()]
        annual_returns_data = [('year', 'return')] + annual_returns_data

        index_dict = self.get_index_dict(format=True)
        data = [(x, y) for x, y in index_dict.items()]
        data = [('index', 'value')] + data

        content = list()
        content.append(Paragraph('Sector: ' + self._sector, styles['Heading1']))
        # content.append(Graphs.draw_text('items: ' + ','.join(self._name_list)))
        content.append(Graphs.draw_table(*data,ALIGN='LEFT',VALIGN='RIGHT',col_width = [100,100]))
        content.append(Spacer(0, 0.5 * cm))
        content.append(Graphs.draw_text('Cumulative_returns:'))

        fig = plt.figure(figsize=(5, 2.5))
        ax = fig.add_subplot(111)
        self.plot_cumulative_returns(show=None, ax=ax)
        imgdata = BytesIO()
        fig.savefig(imgdata, format='svg')
        imgdata.seek(0)  # rewind the data
        drawing = svg2rlg(imgdata)
        content.append(drawing)
        plt.close()
        content.append(Spacer(0, 0.5 * cm))

        content.append(Graphs.draw_text('annual_returns:'))
        content.append(Graphs.draw_table(*annual_returns_data,ALIGN='LEFT',VALIGN='RIGHT',col_width = [100,100]))

        fig = plt.figure(figsize=(5, 2.5))
        ax = fig.add_subplot(111)
        self.plot_annual_returns(show=None, ax=ax)
        imgdata = BytesIO()
        fig.savefig(imgdata, format='svg')
        imgdata.seek(0)  # rewind the data
        drawing = svg2rlg(imgdata)
        content.append(drawing)
        plt.close()
        content.append(Spacer(0, 0.5 * cm))

        return content

    @staticmethod
    def process_daily_list(daily_list,by_mean = True):
        df = pandas.concat(daily_list, axis=1).sort_index() # .fillna(0) 去掉0值填充，避免在早期品种不多时候，被平均之后，波动率虚假偏低
        df.index = pandas.to_datetime(df.index)
        if by_mean:
            daily = df.mean(axis=1)
        else:
            daily = df.sum(axis=1)
        return daily

    @staticmethod
    def get_daily_return(settle_obj_list,by_mean=True):
        daily_list = [each.daily_return for each in settle_obj_list]
        daily = SectorAnalysis.process_daily_list(daily_list=daily_list,by_mean=by_mean)
        return daily,daily_list

    @staticmethod
    def get_daily_pnl_gross(settle_obj_list,by_mean=True):
        daily_list = [each.daily_pnl_gross for each in settle_obj_list]
        daily = SectorAnalysis.process_daily_list(daily_list=daily_list,by_mean=by_mean)
        return daily,daily_list

    @staticmethod
    def get_daily_pnl_fee(settle_obj_list,by_mean=True):
        daily_list = [each.daily_pnl_fee for each in settle_obj_list]
        daily = SectorAnalysis.process_daily_list(daily_list=daily_list,by_mean=by_mean)
        return daily,daily_list

    @staticmethod
    def get_daily_return_by_init_aum(settle_obj_list,by_mean=True):
        daily_list = [each.daily_return_by_init_aum for each in settle_obj_list]
        daily = SectorAnalysis.process_daily_list(daily_list=daily_list,by_mean=by_mean)
        return daily,daily_list

    @staticmethod
    def get_daily_pnl(settle_obj_list,by_mean=True):
        daily_list = [each.daily_pnl for each in settle_obj_list]
        daily = SectorAnalysis.process_daily_list(daily_list=daily_list,by_mean=by_mean)
        return daily,daily_list

    @staticmethod
    def load_settle_obj(temp_path):
        settle_obj_dict = {}
        for x in os.walk(temp_path):
            if 'settle_obj_' in x[2]:
                settle_obj = pickle.load(open(os.path.join(x[0],x[2]),'rb'))
                settle_obj_dict[int(x[2].replace('settle_obj_','').replace('.pkl',''))] = settle_obj
        settle_obj_list = settle_obj_dict.values()
        return settle_obj_list

    @staticmethod
    def hist_index(daily_returns_list,name_list,index_func,**kwargs):
        index_list = [ index_func(daily_returns,**kwargs) for daily_returns in daily_returns_list]
        plt.hist(zip(name_list,index_list))
        plt.show()
