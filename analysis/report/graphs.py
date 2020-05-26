from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Table, SimpleDocTemplate, Paragraph
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.legends import Legend
from reportlab.graphics.shapes import Drawing
#注册字体
# pdfmetrics.registerFont(TTFont('SimSun', 'SimSun.ttf'))

class Graphs:
    def __init__(self):
        pass
    #绘制标题
    @staticmethod
    def draw_title(titlestr='title'):
        style = getSampleStyleSheet()
        ct = style['Normal']
        # ct.fontName = 'SimSun'
        ct.fontSize = 18
        #设置行距
        ct.leading = 50
        #颜
        ct.textColor = colors.black
        #居中
        ct.alignment = 1
        #添加标题并居中
        title = Paragraph(titlestr, ct)
        return title



    #绘制内容

    @staticmethod
    def draw_text(text='text',fontSize=14):
        style = getSampleStyleSheet()
        #常规字体(非粗体或斜体)
        ct = style['Normal']
        #使用的字体s
        # ct.fontName = 'SimSun'
        ct.fontSize = fontSize
        #设置自动换行
        ct.wordWrap = 'CJK'
        #居左对齐
        ct.alignment = 0
        #第一行开头空格
        ct.firstLineIndent = 32
        #设置行距
        ct.leading = 30
        text = Paragraph(text, ct)
        return text



    #绘制表格
    @staticmethod
    def draw_table(*args,ALIGN='CENTER',VALIGN='MIDDLE',col_width=None):
        col_width_0 = 60
        if col_width is not None:
            col_width_0 = col_width

        style = [
        ('FONTNAME', (0, 0), (-1, -1), 'SimSun'),#字体
        ('BACKGROUND', (0, 0), (-1, 0), '#d5dae6'),#设置第一行背景颜色
        ('ALIGN', (0, 0), (-1, -1), ALIGN),#对齐
        ('VALIGN', (-1, 0), (-2, 0), VALIGN),#对齐
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),#设置表格框线为grey色，线宽为0.5
        ]
        table = Table(args, colWidths=col_width_0, style=style,hAlign='LEFT')
        return table

    @staticmethod
    def draw_plot(data=[]):
        drawing = Drawing(400, 200)

        lp = LinePlot()
        lp.x = 50
        lp.y = 50
        lp.height = 125
        lp.width = 300
        lp.data = data # [zip(times, pred), zip(times, high), zip(times, low)]
        lp.lines[0].strokeColor = colors.blue
        lp.lines[1].strokeColor = colors.red
        lp.lines[2].strokeColor = colors.green

        drawing.add(lp)
        return drawing

    #创建图表
    @staticmethod
    def draw_bar(bar_data=[], ax=[], items=[]):
        drawing = Drawing(500, 250)
        bc = VerticalBarChart()
        bc.x = 35
        bc.y = 100
        bc.height = 120
        bc.width = 350
        bc.data = bar_data
        bc.strokeColor = colors.black
        bc.valueAxis.valueMin = 0
        bc.valueAxis.valueMax = 100
        bc.valueAxis.valueStep = 10
        bc.categoryAxis.labels.dx = 8
        bc.categoryAxis.labels.dy = -10
        bc.categoryAxis.labels.angle = 20
        bc.categoryAxis.categoryNames = ax

        #图示

        leg = Legend()
        # leg.fontName = 'SimSun'
        leg.alignment = 'right'
        leg.boxAnchor = 'ne'
        leg.x = 465
        leg.y = 220
        leg.dxTextSpace = 10
        leg.columnMaximum = 3
        leg.colorNamePairs = items
        drawing.add(leg)
        drawing.add(bc)
        return drawing

if __name__ == "__main__":

    content = list()
    #添加标题
    content.append(Graphs.draw_title())
    #添加段落
    content.append(Graphs.draw_text())
    #添加表格数据
    data = [('兴趣', '2019-1', '2019-2', '2019-3', '2019-4', '2019-5', '2019-6'),
    ('开发', 50, 80, 60, 35, 40, 45),
    ('编程', 25, 60, 55, 45, 60, 80),
    ('敲代码', 30, 90, 75, 80, 50, 46)]
    content.append(Graphs.draw_table(*data))
    #添加图表
    b_data = [(50, 80, 60, 35, 40, 45), (25, 60, 55, 45, 60, 80), (30, 90, 75, 80, 50, 46)]
    ax_data = ['2019-1', '2019-2', '2019-3', '2019-4', '2019-5', '2019-6']
    leg_items = [(colors.red, '开发'), (colors.green, '编程'), (colors.blue, '敲代码')]
    content.append(Graphs.draw_bar(b_data, ax_data, leg_items))
    #生成pdf文

    doc = SimpleDocTemplate('report.pdf', pagesize=letter)

    doc.build(content)