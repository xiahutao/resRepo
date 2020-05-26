# coding=utf-8
import warnings
warnings.filterwarnings("ignore")
import importlib,sys
importlib.reload(sys)
import os, re
import csv
from arctic import Arctic, TICK_STORE
import multiprocessing
import subprocess
import datetime
from arctic.exceptions import *
import pandas as pd
from data_engine.data_factory import DataFactory
import pymongo, datetime
import ftplib
import socket
from data_engine.setting import *
from jqdatasdk import *
"""各渠道下载数据的引擎"""


class EngineBase:
    """引擎基类"""
    def __init__(self, host, username, passwd):
        self.link = self.connect(host, username, passwd)

    def connect(self, host, username, passwd):
        """连接"""
        pass

    def downLoad(self, dataName, callback):
        """从网络获取文件的基类"""
        pass

    def getFileList(self):
        """获取数据列表"""
        pass

    def close(self):
        """关闭连接"""
        pass


class FTPengine(EngineBase):
    """FTP数据引擎"""

    def connect(self, host, username, passwd):
        """连接"""
        try:
            link = ftplib.FTP(host)
        except (socket.error, socket.gaierror) as e:
            print('Error, cannot reach ' + host)
            return
        else:
            print('Connect To Host Success...')

        try:
            link.login(username, passwd)
        except ftplib.error_perm:
            print('Username or Passwd Error')
            link.quit()
            return
        else:
            print('Login Success...')

        return link

    def downLoad(self, dataName, callback):
        """Ftp文件下载，callback为回调函数"""
        try:
            print(dataName, 'Downloading')
            self.link.retrbinary(f'RETR {dataName}', callback)
        except ftplib.error_perm as e:
            print(e, 'File Error')
            #os.unlink(localpath)
        else:
            print(dataName, 'Download Success...')

    def ftpUpload(self, remotepath, localpath):
        """上传数据"""
        try:
            self.link.storbinary('STOR %s' % remotepath, open(localpath, 'rb'))
        except ftplib.error_perm:
            print('File Error')
            os.unlink(localpath)
        else:
            print('Upload Success...')

    def getFileList(self):
        """获得数据列表"""
        return self.link.nlst()

    def close(self):
        self.link.quit()


class Logger:
    """日志， 向数据库读写"""
    cl = DataFactory.get_mongo_client()
    db = cl["updateLog"]

    @classmethod
    def insertFutureLogToDB(cls, col='future', **kwargs):
        """生成日志插入数据库, date和symbol为必须"""
        col = cls.db[col]
        date = kwargs.get('date', None)
        symbol = kwargs.get('symbol', None)
        assert date is not None and symbol is not None
        try:
            log = list(col.find({"date": date, "symbol": symbol}))[-1]
        except:
            log = None
        if not log:
            s = kwargs.get('s', None)
            e = kwargs.get('e', None)
            tick = kwargs.get('tick', 0)
            oneMin = kwargs.get('oneMin', 0)
            fiveMin = kwargs.get('fiveMin', 0)
            day = kwargs.get('day', 0)
            original = kwargs.get('original', 0)
        else:
            s = kwargs.get('s', log['s'])
            e = kwargs.get('e', log['e'])
            tick = kwargs.get('tick', log['tick'])
            oneMin = kwargs.get('oneMin', log['oneMin'])
            fiveMin = kwargs.get('fiveMin', log['fiveMin'])
            day = kwargs.get('day', log['day'])
            original = kwargs.get('original', log['original'])
        t = pd.to_datetime(datetime.datetime.now()).tz_localize('PRC')
        log = {'date': date,
               'symbol': symbol,
               's': s,
               'e': e,
               'tick': tick,
               'oneMin': oneMin,
               'fiveMin': fiveMin,
               'day': day,
               'original': original,
               'updatetime': t
               }
        r = col.update_one(filter={'date': date, 'symbol': symbol},
                           update={'$set': log}, upsert=True)
        result = r.raw_result['ok']
        cls.cl.close()
        return result

    @classmethod
    def insertMContractLogToDB(cls, col='mainContract', **kwargs):
        """制作主力合约的日志"""
        col = cls.db[col]
        mark_start = kwargs.get('mark_start', None)
        mark_end = kwargs.get('mark_end', None)
        last_mark_end = kwargs.get('last_mark_end', None)
        symbol = kwargs.get('symbol', None)
        assert symbol is not None and mark_end is not None
        try:
            log = list(col.find({"last_mark_end": last_mark_end, "symbol": symbol}))[-1]
        except:
            log = None
        if not log:
            tick = kwargs.get('tick', 0)
            oneMin = kwargs.get('oneMin', 0)
            fiveMin = kwargs.get('fiveMin', 0)
            day = kwargs.get('day', 0)
        else:
            tick = kwargs.get('tick', log['tick'])
            oneMin = kwargs.get('oneMin', log['oneMin'])
            fiveMin = kwargs.get('fiveMin', log['fiveMin'])
            day = kwargs.get('day', log['day'])
        t = pd.to_datetime(datetime.datetime.now()).tz_localize('PRC')
        lognew = {
            'symbol': symbol,
            'mark_start': mark_start,
            'mark_end': mark_end,
            'last_mark_end': last_mark_end,
            'tick': tick,
            'oneMin': oneMin,
            'fiveMin': fiveMin,
            'day': day,
            'updatetime': t
        }
        r = col.update_one(filter={"mark_start": mark_start, "mark_end": mark_end, 'symbol': symbol},
                           update={'$set': lognew}, upsert=True)
        result = r.raw_result['ok']
        cls.cl.close()
        return result

    @classmethod
    def getLog(cls, condition, col='future'):
        """按条件获取日志"""
        col = cls.db[col]
        df = pd.DataFrame(col.find(condition))
        cls.cl.close()
        return df


if __name__ == '__main__':
    # date = datetime.datetime.now().strftime('%Y-%m-%d')
    # symbol = 'test'
    # mc = pymongo.MongoClient('mongodb://juzheng:password@192.168.2.201:27017/')
    # store = Arctic(mc)
    # lib201 = store['future_tick']
    # d = lib201.read_metadata('M1907')
    # s = d['s']
    # e = d['e']
    # res = Logger.insertFutureLogToDB(col='future', date=date, symbol=symbol, day=0, oneMin=1, e=e, s=s)
    # print(res)
    f = FTPengine(host='192.168.2.206', username='ftp_juzheng', passwd='password')


