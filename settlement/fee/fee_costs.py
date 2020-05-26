#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/12/5 15:46
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm
import pickle
import numpy
import os
import pandas as pd
from settlement.fee.fee import Fee

class Fee_costs(Fee):
    def __init__(self):
        Fee.__init__(self)
        with open(os.path.join(os.path.split(os.path.realpath(__file__))[0],'costs.pkl'), 'rb') as handle:
            self.costs_df = pickle.load(handle)

    def calc_fee_ex(self, transaction_df):
        self.costs_df.loc[self.costs_df['BROKER_FEE'].isna(),'BROKER_FEE'] = self.costs_df.loc[self.costs_df['BROKER_FEE'].isna(),'EXCHANGE_FEE'] *1.1
        self.costs_df.loc[self.costs_df['BROKER_FEE_RATE'].isna(),'BROKER_FEE_RATE'] = self.costs_df.loc[self.costs_df['BROKER_FEE_RATE'].isna(),'EXCHANGE_FEE_RATE'] *1.1
        self.costs_df.loc[self.costs_df['BROKER_FEE'].isna(),'BROKER_FEE'] = 0
        self.costs_df.loc[self.costs_df['BROKER_FEE_RATE'].isna(),'BROKER_FEE_RATE'] = 0
        transaction_df = pd.merge(transaction_df,self.costs_df[['BROKER_FEE','BROKER_FEE_RATE']],how='left',right_index=True,left_on='product_id')

        transaction_df['fee'] = numpy.abs(transaction_df['transactions'] * transaction_df['transaction_price'] * transaction_df['contract_size'])  \
                                                                                        * transaction_df['BROKER_FEE_RATE'] +numpy.abs(transaction_df['transactions']) * transaction_df['BROKER_FEE']
        return transaction_df['fee']

    def calc_fee(self, product_id, volumn, price, contract_size, is_open=True):
        mkt = product_id.upper()

        exchFee = self.costs_df.loc[mkt, 'EXCHANGE_FEE']
        exchFeeRate = self.costs_df.loc[mkt, 'EXCHANGE_FEE_RATE']
        brokerFee = self.costs_df.loc[mkt, 'BROKER_FEE']
        brokerFeeRate = self.costs_df.loc[mkt, 'BROKER_FEE_RATE']
        tickSize=self.costs_df.loc[mkt,'TICK_SIZE']
        trd = volumn * price * contract_size
        commissionsDollar = None
        bidAskDollar = None

        if not pd.isnull(exchFee):  # if it is quoted in absolute
            if pd.isnull(brokerFee):
                brokerFee = 1.1 * exchFee
            commissionsDollar = abs(trd) * (exchFee * 0 + 1 * brokerFee)
        else:
            if pd.isnull(brokerFeeRate):
                brokerFeeRate = 1.1 * exchFeeRate
            commissionsDollar = abs(trd)  * (exchFeeRate * 0 + 1 * brokerFeeRate)

        # print(commissionsDollar, '......is')
        bidAskDollar = abs(trd) * 0.5 * 1

        return commissionsDollar + bidAskDollar * 0

