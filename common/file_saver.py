#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/12/10 9:16
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import threading
from common.singleton import Singleton
from common.decorator import runing_time
import pickle

class file_saver(object,metaclass=Singleton):
    def __init__(self):
        self.ts = []

    def _pickle(self,data_df,filename):
        with open(filename,'wb') as f:
            pickle.dump(data_df,f)
            f.close()

    def _to_csv(self,data_df,filename):
        data_df.to_csv(filename)

    def pickle_file(self,data_df,filename):
        t = threading.Thread(target=self._pickle,args=(data_df,filename))
        t.setDaemon(True)
        t.start()
        self.ts.append(t)
        
    def save_file(self,data_df,filename):
        t = threading.Thread(target=self._to_csv,args=(data_df,filename))
        t.setDaemon(True)
        t.start()
        self.ts.append(t)
        
    @runing_time
    def join(self):
        for t in self.ts:
            t.join()
