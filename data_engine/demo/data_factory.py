
import logging

from data_engine.data_factory import DataFactory
from data_engine.setting import  DATASOURCE_REMOTE,DATASOURCE_LOCAL
from data_engine.setting import ASSETTYPE_FUTURE,ASSETTYPE_STOCK
from data_engine.setting import FREQ_TICK,FREQ_1D,FREQ_1M,PRICE_TYPE_POST,PRICE_TYPE_PRE,PRICE_TYPE_UN
from data_engine.market_tradingdate import Market_tradingdate

if __name__ == '__main__':
    products = ['C', 'A', 'Y', 'M', 'P', 'L', 'V', 'J', 'JM', 'JD', 'I', 'PP', 'CS',
                'AP', 'CF', 'FG', 'MA', 'OI', 'RM', 'SF', 'SM', 'SR', 'TA', 'WH', 'ZC',
                'AG', 'AL', 'AU', 'CU', 'NI', 'RB', 'RU', 'ZN', 'BU', 'PB', 'HC', 'SN']
    # from data_engine.setting import *
    # from data_engine.global_variable import *
    # mongoclient = DataFactory.get_mongo_client()
    # db = mongoclient.get_database('MARKET')
    # instruments = db.get_collection('instruments')
    # d = DataFactory()
    #
    # instrument_df = DataFactory._get_instruments(asset_type=ASSETTYPE_FUTURE)
    # instrument_df.loc[:,'ProductID'] = instrument_df['ProductID'].str.upper()
    # instrument_df.dropna(subset=['PriceTick'],inplace=True)
    # instrument_df_new = instrument_df.pivot_table(index='ProductID',values='PriceTick',aggfunc='last')
    # for idx,row in instrument_df_new.iterrows():
    #     if len(idx) == 0:
    #         continue
    #     instruments.update_one({'symbol':idx},update={'$set':{'PriceTick':row['PriceTick']}},upsert=False)
    #     instruments.update_one({'symbol':idx + '_S'},update={'$set':{'PriceTick':row['PriceTick']}},upsert=False)
    #     instruments.update_one({'symbol':idx + '_S_VOL'},update={'$set':{'PriceTick':row['PriceTick']}},upsert=False)
    #     instruments.update_one({'symbol':idx + '_VOL'},update={'$set':{'PriceTick':row['PriceTick']}},upsert=False)
    # print(instrument_df_new)
    # instr = DataFactory()._get_instruments()
    import datetime
    from data_engine.market_tradingdate import Market_tradingdate
    import pytz
    import data_engine.global_variable as global_variable
    import pandas.tseries.holiday as holiday

    DataFactory.config(MONGDB_PW='jz501241', MONGDB_USER='dbmanager_future',
                       DATASOURCE_DEFAULT=global_variable.DATASOURCE_REMOTE)
    sr = DataFactory.get_future_market_data(freq=global_variable.FREQ_1D, symbols='SR_VOL',start_date=None,end_date=None)
    print(sr['SR_VOL'])
    print(sr['SR_VOL'].iloc[-1])
    from data_engine.instrument.product import Product
    p = Product(product_id='SR')
    p.load_hq()
    print(p.is_max_volume_symbol_changed())
    import pymongo
    # mc = pymongo.MongoClient()
    # mc = DataFactory.get_mongo_client()
    # db = mc.get_database('portfolio')
    # cl = db.get_collection('strategy_opt_weight')
    # # cl.delete_many({'lookback':'seasonality_3_year'})
    #
    # db = mc.get_database('MARKET')
    # cl = db.get_collection('product')
    # cl.update_one(filter= {'ProductID':'C'},update={'$set':{('$contract.$.'+ 'ctest'):{'a':'a'}}})

    # for i in range(20):
    #     DataFactory()._get_instruments()
    # df = DataFactory.get_future_market_data(freq=global_variable.FREQ_1D,symbols='AL_VOL',start_date=datetime.datetime(2004,6,18,18),end_date=datetime.datetime(2004,7,19,18))
    # print(df['AL_VOL'])
    # df = DataFactory.get_future_market_data(freq=global_variable.FREQ_1D,symbols='AL0904',start_date=datetime.datetime(2009,1,15,18),end_date=datetime.datetime(2009,1,19,18))
    # print(df['AL0904'])
    # import pymongo
    # mongoclient = DataFactory.get_mongo_client()
    # assert isinstance(mongoclient,pymongo.MongoClient)
    # db = mongoclient.get_database('portfolio')
    # # db2 = mongoclient.get_database('portfolio')
    # cli = db.get_collection('strategy')
    # result = cli.find({},{'_id':0})
    # for each in result:
    #     if len(each['aggtokens']) == 0:
    #         continue
    #     aggtokens = each['aggtokens']
    #     aggtokens2 = {x:{'weight':v,'target_vol':0.15} for x,v in aggtokens.items()}
    #     cli.update_one({'strategy':each['strategy']}, update={'$set': {'aggtokens':aggtokens2}})
    #     print(each)


    # cli2 = db.get_collection('account')
    # result = cli2.find_one({},{'_id':0})
    # print(result)
    # for each in result['strategy']:
    #     print(each)
    #     result['strategy'][each].pop('aggtokens')
    #     print(result['strategy'][each])
    # print(result)
    # cli2.update_one({'account':"simnow-1"},update={'$set':result})
        # cli2.insert({'strategy':each,'aggtokens':result['strategy'][each]['aggtokens']})

    # db.get_collection('account_bak').insert(result)
    # for each in result['strategy']:
    #     print(each)
    #     print(result['strategy'][each])
    #     cli2.insert({'strategy':each,'aggtokens':result['strategy'][each]['aggtokens']})

    # db2 = mongoclient.get_database('Trading_record')
    # cli2 = db2.get_collection('ORDER_REQUESTS')
    # result2 = cli2.find()
    # strategy_aggtokens = {'YMJH-Daily':set(),'BaseMomentum-Daily':set()}
    # for x in result2:
    #     account = x['account']
    #     strategy = x['strategy']
    #     if 'aggToken' not in x:
    #         continue
    #     aggToken = x['aggToken']
    #     if 'Pair-Intraday' == strategy:
    #         if '_VOL' not in aggToken:
    #             continue
    #     if strategy not in strategy_aggtokens:
    #         strategy_aggtokens[strategy] = set()
    #     strategy_aggtokens[strategy].add(aggToken)
    #
    # db = mongoclient.get_database('account')
    # cli = db.get_collection('account_capital')
    # result = cli.find_one({'account':'simnow-1'})
    # result.pop('_id')
    # print(result)
    #
    # result = {'account':'simnow-1','capital':5000000.0,'strategy':{}}
    # for each_strategy, aggtokens in strategy_aggtokens.items():
    #     if len(aggtokens) == 0:
    #         result['strategy'][each_strategy] = {'aggtokens': {}, 'weight': 1.0 / 6,
    #                                              'target_vol': 0.15}
    #     else:
    #         w = 1.0 / len(aggtokens)
    #         result['strategy'][each_strategy] = {'aggtokens':{x:w for x in aggtokens},'weight':1.0 / 6,'target_vol':0.15}
    #
    # db.get_collection('account_capital').update_one({'account': 'simnow-1'}, update={'$set': result}, upsert=True)
            # total_weigth += result['strategy'][each_strategy]['aggtokens'][eachtoken]
    # total_weigth = 0
    # for each_strategy, aggtokens in strategy.items():
    #     for eachtoken in aggtokens:
    #         result['strategy'][each_strategy][eachtoken] = 20.0/ 5000.0
    #         total_weigth += result['strategy'][each_strategy][eachtoken]
    # print(total_weigth)

    #     if strategy not in result['strategy']:
    #         result['strategy'][strategy] = {}
    #     if aggToken not in result['strategy'][strategy]:
    #         result['strategy'][strategy][aggToken] = 0
    # print(result)
    # result = cli2.distinct(key=['account','strategy','aggToken'])
    # cli2.group(key=)
    # print(result)
    # md = DataFactory.get_future_market_data(freq=global_variable.FREQ_1D,symbols=['AL_VOL'],start_date=None,end_date=None)
    # print(md['AL_VOL'].tail())
    # print(md['AL_VOL'].iloc[-1])

    # marketdate = Market_tradingdate(ExchangeID='SHE')
    # marketdate.get_hisdata()
    # print(marketdate.isBusinessday(date='2020-02-28'))
    # print(marketdate.isBusinessday(date='2020-02-08'))
    # print(marketdate.isBusinessday(date=datetime.datetime.now()))
    # md = DataFactory().get_future_market_data(freq=global_variable.FREQ_5M,symbols='RB2005',start_date=None,end_date=None,use_start_time_as_index=True)
    # dt = md['RB2005']
    # print(dt)
    # end_time = (datetime.datetime.now().replace(hour=18, minute=0, second=0, microsecond=0) + datetime.timedelta(-0)).astimezone(pytz.timezone(global_variable.DEFAULT_TIMEZONE))
    # start_time = holiday.previous_friday(end_time + datetime.timedelta(-1))
    # mkt =  Market_tradingdate(ExchangeID='SHE')
    # print( mkt.get_next_trading_date(date=end_time.strftime('%Y%m%d')))
    # print('get_next_trading_date')
    # print( mkt.get_next_trading_date(date='20200117'))
    # print( mkt.get_next_trading_date(date='20200118'))
    # print( mkt.get_next_trading_date(date='20200119'))
    # print( mkt.get_next_trading_date(date='20200120'))
    # print( mkt.get_next_trading_date(date='20200121'))
    #
    # print('get_last_trading_date')
    # print( mkt.get_last_trading_date(date='20200117',include_current_date=True))
    # print( mkt.get_last_trading_date(date='20200118',include_current_date=True))
    # print( mkt.get_last_trading_date(date='20200119',include_current_date=True))
    # print( mkt.get_last_trading_date(date='20200120',include_current_date=True))
    # print( mkt.get_last_trading_date(date='20200121',include_current_date=True))
    # print(DataFactory.get_market_dict (symbols='I_VOL'))
    # md = DataFactory().get_future_market_data(freq='1m',symbols='I_VOL',start_date=None,end_date=None)
    # print(md['I_VOL'])
    # tmp = DataFactory.get_strategy_log(strategy_type='PairStrategy')

    # DataFactory.sync_future_from_remote(symbol='AL',freq=FREQ_1D)
    # for i in range(10):
    #     ret = DataFactory.get_contract_size_dict(symbols=['AL','RB'])
    #     print(ret)
    # start_date = '2010-01-01'
    # ins = DataFactory().get_market_data_by_product(asset_type=ASSETTYPE_FUTURE,freq=FREQ_1M,product= 'RB'
    #                                                ,start_date=start_datCUe)
    # data = DataFactory().get_future_market_data(freq=FREQ_TICK,symbols='T1912',start_date=start_date,end_date=None)
    # print(data['T1912'])
    # print(data['T1912'].iloc[-1])
    # products=['CU']
    # for productid in products:
    #     productid_vol = productid + '_VOL'
    #     future_data = DataFactory().get_future_market_data(freq=FREQ_1M,symbols=productid_vol,start_date='2019-07-01',end_date=None)
    #     df = future_data[productid_vol]
    #     print(productid_vol,df.head())
    #
    #     future_data = DataFactory().get_future_market_data(freq=FREQ_1M,symbols=productid,start_date=None,end_date=None)
    #     df = future_data[productid]
    #     print(productid,df.head())
    # for i in range(5):
    #     DataFactory().get_trading_date()
    # # market_data = DataFactory().get_market_data(asset_type=ASSETTYPE_FUTURE, freq=FREQ_1M, symbols=['ZC_VOL'], price_type=PRICE_TYPE_UN,
    # #                             start_date='20151030', end_date='20151101')
    # # df = market_data['ZC_VOL']
    # # print(df.head())
    # # print(market_data['HC_VOL'].iloc[-1])
    #
    #
    # future_data = DataFactory().get_stock_market_data()