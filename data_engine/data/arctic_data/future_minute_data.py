
import pandas
import datetime
import pytz
from data_engine.data.future_data import FutureData
from data_engine.setting import FREQ_1M,FUTURE_MINUTE_LIB,FREQ_5M,FUTURE_MINUTE_5_LIB,DEFAULT_TIMEZONE
import data_engine.setting as Setting
from arctic import Arctic
from arctic.date import DateRange
from arctic.exceptions import NoDataFoundException


class FutureMinuteData(FutureData):
    """
    期货分钟数据接口，从mongo读取
    """
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        FutureData.__init__(self, freq=FREQ_1M,price_type=price_type,**kwargs)

    def _get_arclib_str(self):
        return FUTURE_MINUTE_LIB

    @staticmethod
    def _format_data(df):
        if df is not None and not df.empty:
            df.index = pandas.to_datetime(df.index, utc=True)
            df.index = df.index.tz_convert('Asia/Shanghai')
            df.sort_index(inplace=True)
            df = df[~df.index.duplicated()]
            # df.rename(columns={'open': 'OPEN_PX', 'close': 'CLOSE_PX', 'high': 'HIGH_PX', 'low': 'LOW_PX',
            #                    'volumn': 'VOLUME'}, inplace=True)
            #df = df[df['VOLUME'] > 0]

            # df['LAST_CLOSE_PX'] = df['CLOSE_PX'].shift(1)
            # if 'contract_id' in df.columns:
            #     last_contract_id = df['contract_id'].shift(1)
            #     df.loc[:,'LAST_CLOSE_PX'].where(last_contract_id == df['contract_id'],df['last_close'],inplace=True)
            # df['LAST_VOLUME'] = df['VOLUME'].shift(1)
            # df['PRICE_RETURN'] = df['CLOSE_PX'] / df['LAST_CLOSE_PX'] - 1
            # df['PRICE_RETURN'].fillna(0, inplace=True)
            # df['PRICE_DELTA'] = df['CLOSE_PX'] - df['LAST_CLOSE_PX']
            # df['PRICE_DELTA'].fillna(0, inplace=True)

        return df

    def __read_data(self,symbol_tmp,chunk_range,use_start_time_as_index=False):
        try:
            ret = self.arc_lib.read(symbol_tmp,chunk_range)
            if use_start_time_as_index:
                ret.index = pandas.DatetimeIndex(ret['start_time'],tz=pytz.timezone(DEFAULT_TIMEZONE))
                ret.index.name = 'index'
            return ret
        except NoDataFoundException:
            return None
    def _get_data(self, symbol, start_date, end_date, **kwargs):
        chunk_range = DateRange(start_date,end_date)
        use_start_time_as_index = False
        if 'use_start_time_as_index' in kwargs:
            use_start_time_as_index = kwargs['use_start_time_as_index']
        if isinstance(symbol,str):
            try:
                df = self.__read_data(symbol,chunk_range,use_start_time_as_index)
                df = FutureMinuteData._format_data(df)
                return df[start_date: end_date]
            except NoDataFoundException:
                #print((symbol_tmp, 'NoDataFoundException',start_date,end_date))
                pass
        elif isinstance(symbol, list) and len(symbol) == 1:
            df = self.__read_data(symbol[0], chunk_range,use_start_time_as_index)
            if df is not None:
                df = FutureMinuteData._format_data(df)
                return df[start_date: end_date]
        elif isinstance(symbol,list):
            df_dict = { symbol_tmp: self.__read_data(symbol_tmp,chunk_range,use_start_time_as_index) for symbol_tmp in symbol}
            if df_dict is None:
                return None
            elif isinstance(df_dict, dict):
                df_dict = {x:FutureMinuteData._format_data(y) for x,y in df_dict.items() if not y is None and not y.empty}
                return df_dict
            elif isinstance(df_dict, pandas.DataFrame):
                df_dict = FutureMinuteData._format_data(df_dict)
                return df_dict[start_date: end_date]

        # elif isinstance(symbol,list):
        #     df_dict = {}
        #     for symbol_tmp in symbol:
        #         try:
        #             df = self.arc_lib.read(symbol_tmp, chunk_range)
        #             df = FutureMinuteData._format_data(df)
        #             df_dict[symbol_tmp] = df[start_date: end_date]
        #         except NoDataFoundException:
        #             # print((symbol_tmp, 'NoDataFoundException',start_date,end_date))
        #             pass
        #     return df_dict
        return None


class Future5MinuteData(FutureMinuteData):
    def __init__(self,price_type=Setting.PRICE_TYPE_UN,**kwargs):
        FutureData.__init__(self,price_type=price_type, freq=FREQ_5M,**kwargs)

    def _get_arclib_str(self):
        return FUTURE_MINUTE_5_LIB