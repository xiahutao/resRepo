# -*- coding: utf-8 -*-

#全局变量
# FUTURE_TICK_LIB = 'future_tick'
FUTURE_TICK_LIB = 'future_tick'
FUTURE_DAILY_LIB = 'future_1d'
FUTURE_MINUTE_5_LIB = 'future_5min'
FUTURE_MINUTE_LIB = 'future_1min'

STOCK_MINUTE_5_LIB = 'stock_raw.stock_5min'
STOCK_MINUTE_LIB = 'stock_raw.stock_minute'
STOCK_DAILY_LIB = 'stock_raw.stock_1d'
STOCK_DAILY_JQ_LIB = 'stock_raw.stock_1d_jq'
STOCK_DAILY_PRE_JQ_LIB = 'stock_raw.stock_1d_jq_pre'
STOCK_DAILY_POST_JQ_LIB = 'stock_raw.stock_1d_jq_post'
STOCK_DAILY_PRE_LIB = 'stock_raw.stock_1d_pre'
STOCK_DAILY_POST_LIB = 'stock_raw.stock_1d_post'
STOCK_TICK_LIB = 'stock'

INDEX_WEIGHT_LIB = 'index_weight'

BOND_MINUTE_LIB = 'bond_minute'
BOND_DAILY_LIB = 'bond_1d'
BOND_TICK_LIB = 'bond_tick'

FEATURE_DAILY_LIB = 'feature_1d'

EXCHANGE_ID_SHF = 'SHF'
EXCHANGE_ID_CZC = 'CZC'
EXCHANGE_ID_DCE = 'DCE'

DATASOURCE_REMOTE = 'DataSource_Remote'
DATASOURCE_LOCAL = 'DataSource_Local'

ASSETTYPE_BOND = 'Bond'
ASSETTYPE_FUTURE = 'Future'
ASSETTYPE_STOCK = 'Stock'
ASSETTYPE_STOCK_FEATURE = 'Stock_feature'
ASSETTYPE_INDEX = 'Index'
ASSETTYPE_INDEX_WEIGHT = 'Index_weight'
ASSETTYPE_ETF = 'ETF'

FREQ_1D = '1d'
FREQ_1W = '1w'
FREQ_1M = '1m'
FREQ_5M = '5m'
FREQ_TICK = 'tick'

DEFAULT_TIMEZONE='PRC'

PRICE_TYPE_PRE = 'pre'
PRICE_TYPE_POST = 'post'
PRICE_TYPE_UN = 'un'

MARK_MC_FST_OI = ''
MARK_MC_SEC_OI = '_S'
MARK_MC_FST_VOL = '_VOL'
MARK_MC_SEC_VOL = '_S_VOL'

import logging
import pytz
def get_timezone(timezone=None):
    global DEFAULT_TIMEZONE
    if timezone is None:
        timezone = DEFAULT_TIMEZONE
    return pytz.timezone(timezone)
import datetime
def get_now(at_15_00_00=True):
    if at_15_00_00:
        return datetime.datetime.now().replace(hour=15,minute=0,second=0,microsecond=0).astimezone(get_timezone())
    return datetime.datetime.now().astimezone(get_timezone())