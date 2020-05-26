#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/12/18 15:57
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import os
import pandas
def check_fold(path):
    if os.path.exists(path):
        return True
    else:
        try:
            os.makedirs(path)
        except (Exception):
            pass
    if os.path.exists(path):
        return True
    return False


def merge_lines(folder,filename_part_list,target_file):
    lines= []
    for x in os.walk(folder):
        for f in x[2]:
            is_file = True
            for partname in filename_part_list:
                if not partname in f:
                    is_file = False
            if not is_file:
                continue

            with open(os.path.join(x[0], f), encoding='utf-8') as file:
                line = file.readline()
                if len(line) == 0:
                    continue
                lines.append(line)
                file.close()
    fp_day = open(target_file, mode='w')
    fp_day.writelines(lines)
    fp_day.close()
    return lines

def get_file_list(path,format_folder_func,filename='daily_returns.csv'):
    from data_engine.instrument.future import Future
    dr_filename_dict = {}
    for x in os.walk(path):
        if filename is None:
            for each in x[2]:
                dr_filename_dict[each] = os.path.join(x[0], each)
        elif filename in x[2]:
            dr_filename_dict[os.path.split(x[0])[-1]] = os.path.join(x[0], filename)

    dr_dict = {format_folder_func(x): y for x, y in dr_filename_dict.items()}
    dr_dict = {x: pandas.read_csv(y, header=None, names=['date_index', x]).set_index('date_index')[x] for x, y in
               dr_dict.items()}
    name_list = dr_dict.keys()
    daily_returns_list = dr_dict.values()
    future_list = [Future(x) for x in name_list]
    sector_list = [x.sector for x in future_list]
    return sector_list,name_list,daily_returns_list
