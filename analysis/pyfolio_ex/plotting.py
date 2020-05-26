#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2019/11/22 14:48
# @Author  : jwliu
# @Site    : 
# @Software: PyCharm

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import empyrical as ep
import analysis.pyfolio_ex.txn as txn

def plot_slippage_sweep(returns, positions, transactions,
                        slippage_params=(3, 8, 10, 12, 15, 20, 50),
                        ax=None, **kwargs):
    """
    Plots equity curves at different per-dollar slippage assumptions.

    Parameters
    ----------
    returns : pd.Series
        Timeseries of portfolio returns to be adjusted for various
        degrees of slippage.
    positions : pd.DataFrame
        Daily net position values.
         - See full explanation in tears.create_full_tear_sheet.
    transactions : pd.DataFrame
        Prices and amounts of executed trades. One row per trade.
         - See full explanation in tears.create_full_tear_sheet.
    slippage_params: tuple
        Slippage pameters to apply to the return time series (in
        basis points).
    ax : matplotlib.Axes, optional
        Axes upon which to plot.
    **kwargs, optional
        Passed to seaborn plotting function.

    Returns
    -------
    ax : matplotlib.Axes
        The axes that were plotted on.
    """

    if ax is None:
        ax = plt.gca()

    slippage_sweep = pd.DataFrame()
    for bps in slippage_params:
        adj_returns = txn.adjust_returns_for_slippage(returns, positions,
                                                      transactions, bps)
        label = str(bps) + " bps"
        slippage_sweep[label] = ep.cum_returns(adj_returns, 1)

    slippage_sweep.plot(alpha=1.0, lw=0.5, ax=ax)

    ax.set_title('Cumulative returns given additional per-dollar slippage')
    ax.set_ylabel('')

    ax.legend(loc='center left', frameon=True, framealpha=0.5)

    return ax


def plot_slippage_sensitivity(returns, positions, transactions,
                              ax=None, **kwargs):
    """
    Plots curve relating per-dollar slippage to average annual returns.

    Parameters
    ----------
    returns : pd.Series
        Timeseries of portfolio returns to be adjusted for various
        degrees of slippage.
    positions : pd.DataFrame
        Daily net position values.
         - See full explanation in tears.create_full_tear_sheet.
    transactions : pd.DataFrame
        Prices and amounts of executed trades. One row per trade.
         - See full explanation in tears.create_full_tear_sheet.
    ax : matplotlib.Axes, optional
        Axes upon which to plot.
    **kwargs, optional
        Passed to seaborn plotting function.

    Returns
    -------
    ax : matplotlib.Axes
        The axes that were plotted on.
    """

    if ax is None:
        ax = plt.gca()

    avg_returns_given_slippage = pd.Series()
    for bps in range(1, 100):
        adj_returns = txn.adjust_returns_for_slippage(returns, positions,
                                                      transactions, bps)
        avg_returns = ep.annual_return(adj_returns)
        avg_returns_given_slippage.loc[bps] = avg_returns

    avg_returns_given_slippage.plot(alpha=1.0, lw=2, ax=ax)
    ax.axhline(
        0,
        color='steelblue',
        linestyle='--',
        lw=3)

    ax.set_title('Average annual returns given additional per-dollar slippage')
    ax.set_xticks(np.arange(0, 100, 10))
    ax.set_ylabel('Average annual return')
    ax.set_xlabel('Per-dollar slippage (bps)')

    return ax
