#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/11/22 14:49
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import numpy
import pandas
from pyfolio.txn import get_txn_vol

def adjust_returns_for_slippage(returns, positions, transactions,
                                slippage_bps):
    """
    Apply a slippage penalty for every dollar traded.

    Parameters
    ----------
    returns : pd.Series
        Daily returns of the strategy, noncumulative.
         - See full explanation in create_full_tear_sheet.
    positions : pd.DataFrame
        Daily net position values.
         - See full explanation in create_full_tear_sheet.
    transactions : pd.DataFrame
        Prices and amounts of executed trades. One row per trade.
         - See full explanation in create_full_tear_sheet.
    slippage_bps: int/float
        Basis points of slippage to apply.

    Returns
    -------
    pd.Series
        Time series of daily returns, adjusted for slippage.
    """

    slippage = 0.0001 * slippage_bps
    positions = numpy.abs(positions)
    portfolio_value = positions.sum(axis=1)
    pnl = portfolio_value * returns
    traded_value = get_txn_vol(transactions).txn_volume
    slippage_dollars = traded_value * slippage
    adjusted_pnl = pnl.add(-slippage_dollars, fill_value=0)

    adjusted_returns = returns * adjusted_pnl / pnl
    adjusted_returns.replace(numpy.inf,0,inplace=True)
    adjusted_returns.replace(-numpy.inf,0,inplace=True)
    return adjusted_returns
