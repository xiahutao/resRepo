#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020/3/18 11:47
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

class dict_helper:
    @staticmethod
    def init_dict(obj, *args):
        if len(args)>0:
            if args[0] not in obj:
                obj[args[0]] = {}
            new_args = args[1:]
            obj[args[0]] = dict_helper.init_dict(obj[args[0]],*new_args)
        return obj

    @staticmethod
    def set_dict(obj,  value, *args):
        obj = dict_helper.init_dict(obj,*args)
        if len(args)>1:
            new_args = args[1:]
            obj[args[0]] = dict_helper.set_dict(obj[args[0]],value,*new_args)
        elif len(args) == 1:
            obj[args[0]] = value
        return obj

    @staticmethod
    def remove_dict(obj,*args):
        if len(args)>1:
            new_args = args[1:]
            obj[args[0]] = dict_helper.remove_dict(obj[args[0]],*new_args)
        elif len(args) == 1:
            obj.pop(args[0])
        return obj

    @staticmethod
    def get_dict(obj, *args):
        obj = dict_helper.init_dict(obj,*args)
        if len(args)>0:
            new_args = args[1:]
            if args[0] not in obj:
                return None
            return dict_helper.get_dict(obj[args[0]],*new_args)
        else:
            return obj
if __name__ == '__main__':

    obj = {}
    obj = dict_helper.init_dict(obj,'a','b','c','d')
    print(obj)
    obj = dict_helper.set_dict(obj,'d',[1,2,3],'a','b','c')
    print(obj)
    obj = dict_helper.get_dict(obj,'a','b','c','d')
    print(obj)