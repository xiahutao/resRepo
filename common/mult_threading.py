#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/12/31 15:01
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import threading


class MyThread(object):
    def __init__(self, func_list=None):
        # 所有线程函数的返回值汇总，如果最后为0，说明全部成功
        self.ret_flag = 0
        self.result_list = []
        self.func_list = func_list
        self.threads = []

    def set_thread_func_list(self, func_list):
        """
        @note: func_list是一个list，每个元素是一个dict，有func和args两个参数
        """
        self.func_list = func_list

    def trace_func(self, func, *args, **kwargs):
        """
        @note:替代profile_func，新的跟踪线程返回值的函数，对真正执行的线程函数包一次函数，以获取返回值
        """
        ret = func(*args, **kwargs)
        self.result_list.append(ret)
        self.ret_flag +=1

    def start(self):
        """
        @note: 启动多线程执行，并阻塞到结束
        """
        self.threads = []
        self.ret_flag = 0
        for func_dict in self.func_list:
            if func_dict["args"]:
                new_arg_list = []
                new_arg_list.append(func_dict["func"])
                for arg in func_dict["args"]:
                    new_arg_list.append(arg)
                new_arg_tuple = tuple(new_arg_list)
                t = threading.Thread(target=self.trace_func, args=new_arg_tuple)
            else:
                t = threading.Thread(target=self.trace_func, args=(func_dict["func"],))
            self.threads.append(t)

        for thread_obj in self.threads:
            thread_obj.start()

        for thread_obj in self.threads:
            thread_obj.join()

    def ret_value(self):
        """
        @note: 所有线程函数的返回值之和，如果为0那么表示所有函数执行成功
        """
        return self.result_list