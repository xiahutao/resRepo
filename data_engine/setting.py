# -*- coding: utf-8 -*-

from data_engine.global_variable import *

# MSSQL 用户名 密码
DB_CONFIG_UID='public_user'
DB_CONFIG_PWD=''

DB_CONFIG_DAILY='DRIVER={SQL Server};SERVER=markets-db.ctfwgeuyhbml.us-west-2.rds.amazonaws.com;DATABASE=MARKET_TEST;UID='+DB_CONFIG_UID+';PWD='+DB_CONFIG_PWD

DB_CONFIG_MINUTE_UID='public_user'
DB_CONFIG_MINUTE_PWD=''
DB_CONFIG_MINUTE='DRIVER={SQL Server};SERVER=robotfin.database.windows.net;DATABASE=MARKET;UID='+DB_CONFIG_MINUTE_UID+';PWD='+DB_CONFIG_MINUTE_PWD

#本地数据文件存储路径（确保文件夹存在）
DAILY_FILE_FOLDER='E://FileDB_DAILY'
MINUTE_FILE_SOURCE='E://FileDB_MINUTE'
MINUTE_FILE_SOURCE_5M='E://FileDB_5MINUTE'
TICK_FILE_SOURCE='E://FileDB_TICK'

#MongoDB
# MONGDB_IP='121.40.237.114' # 阿里云服务器
MONGDB_IP='223.93.165.111' # 居正服务器（杭州）
MONGDB_IP_list = ['223.93.165.111:27017', '223.93.165.111:27018']
MONGDB_USER='juzheng'
MONGDB_PW=''
MONGDB_SET_NAME = "juzheng_backtest"

DATASOURCE_DEFAULT = DATASOURCE_REMOTE

DEVCLOUD_USER = 'juzheng2019'
DEVCLOUD_PWD = ''

logging_level = logging.DEBUG
TEMP=None
