#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/12/30 16:51
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import yaml
f = open('sample.yaml','r',encoding='utf-8')
f_data = f.read()
f.close()
content = yaml.safe_load_all(f_data)
for data in content:
    print(data)



import json

f = open('demo.json','r')
config = json.load(f)
f.close()

f = open('sample2.yaml','w',encoding='utf-8')
yaml.safe_dump(config,f)
ts = yaml.safe_dump(config)
f.close()
print(ts)