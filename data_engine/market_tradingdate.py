#coding=utf-8

import pandas
import numpy
import os
import calendar
import datetime

from data_engine.data_factory import DataFactory

def ToDate(T0=None):
    if T0 is None:
        T0 = datetime.datetime.now()
    T0 = pandas.to_datetime(T0)
    return pandas.to_datetime(T0.date())

class Market_tradingdate(object):
    """
    Todo 直接从Mongo库获取日期，需要封装到data_engine中
    """
    def __init__(self,ExchangeID):
        self.HisData_DF = None

        self.MongoClient = DataFactory.get_mongo_client()

        '''SHFE, '''
        self.ExchangeID = ExchangeID
        self.HisData_DF = None
        self._load_tradingdate()

    def _load_tradingdate(self):
        if self.MongoClient is None:
            return None
        db = self.MongoClient.get_database('Tradedays')
        if self.ExchangeID.upper() == 'DCE':
            collection = db.get_collection('DCE')
        elif self.ExchangeID.upper() == 'CZCE':
            collection = db.get_collection('CZC')
        else:
            collection = db.get_collection('SHF')
        data = collection.find()
        result_list = list(data[:])
        if len(result_list) > 0:
            result_df = pandas.DataFrame(result_list)
            result_df = result_df.drop(columns=['_id'])
            result_df.dropna(subset=['Tradedays_str','Year'],inplace=True)
            result_df['forindex'] = pandas.to_datetime(result_df['Tradedays_str'])
            result_df.drop_duplicates(subset=['forindex'],inplace=True)
            result_df.set_index('forindex',inplace=True)
            result_df.sort_index(inplace=True)
            self.HisData_DF = result_df
            return result_df
        return None

    def GetNWeekday(self,N,year,month,wd=calendar.FRIDAY):
        c = calendar.Calendar(firstweekday=calendar.SUNDAY)
        monthcal = c.monthdatescalendar(year, month)
        third_friday = [day for week in monthcal for day in week if \
                        day.weekday() == wd and \
                        day.month == month][N-1]
        return third_friday

    def isBusinessday(self,date):
        date_str = pandas.to_datetime(date).strftime('%Y-%m-%d')
        df = self.HisData_DF[self.HisData_DF['Tradedays_str'] == date_str]
        if df.empty:
            return None
        return df.iloc[0]['isTradingday']

    def CalcDate(self,fromdate, days, isBusinessday=True):
        fromdate = ToDate(fromdate)
        if not isBusinessday:
            return pandas.to_datetime(fromdate) + datetime.timedelta(days=days)

        hisdata = self.HisData_DF
        hisdata = hisdata[hisdata['isTradingday'] == True]

        if days >= 0:
            hisdata_tmp = hisdata[hisdata.index >= pandas.to_datetime(fromdate)]
            if hisdata_tmp.empty:
                return None
            if days > len(hisdata_tmp):
                return hisdata_tmp.iloc[-1].name
            return hisdata_tmp.iloc[int(days)].name
        else:
            hisdata_tmp = hisdata[hisdata.index < pandas.to_datetime(fromdate)]
            if hisdata_tmp.empty:
                return None
            if abs(days) > len(hisdata_tmp):
                return hisdata_tmp.iloc[0].name
            return hisdata_tmp.iloc[int(days)].name


    def get_hisdata(self,fromdate=None,todate=None,isTradingday=True,close_left_right=True):
        if self.HisData_DF is None:
            self._load_tradingdate()
        ret = self.HisData_DF

        if fromdate is not None:
            if close_left_right:
                ret = ret[ret['Tradedays'].dt.date >= pandas.to_datetime(fromdate).date()]
            else:
                ret = ret[ret['Tradedays'].dt.date > pandas.to_datetime(fromdate).date()]
        if todate is not None:
            if close_left_right:
                ret = ret[ret['Tradedays'].dt.date <= pandas.to_datetime(todate).date()]
            else:
                ret = ret[ret['Tradedays'].dt.date < pandas.to_datetime(todate).date()]
        return ret[ret['isTradingday'] == isTradingday]

    def get_next_trading_date(self,date,include_current_date=False):
        close_left_right = False
        if include_current_date:
            close_left_right = True
        df = self.get_hisdata(fromdate=date,close_left_right=close_left_right)
        if df is None or df.empty:
            return None
        return df.iloc[0].name

    def get_last_trading_date(self,date,include_current_date=False):
        close_left_right = False
        if include_current_date:
            close_left_right = True
        df = self.get_hisdata(todate=date,close_left_right=close_left_right)
        if df is None or df.empty:
            return None
        return df.iloc[-1].name
