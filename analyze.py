import datetime as dt
from statsmodels.tsa.stattools import adfuller
from scipy import stats
import numpy as np
import pandas as pd

import get_data as gd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
np.set_printoptions(linewidth=120, suppress=True)


def analyze(initDate, finalDate, func=gd.getDaily):

    exchange = 'CCCAGG'
    completeOnly = True
    exWeekends = False

    # aggregated hourly price for Bitcoin (2000 row limit - use a loop)
    symbol = 'BTCUSD'
    BTCUSD = func(symbol, initDate, finalDate, exchange, completeOnly, exWeekends)

    symbol = 'LTCBTC'
    LTCBTC = func(symbol, initDate, finalDate, exchange, completeOnly, exWeekends)

    symbol = 'IOTBTC'
    IOTBTC = func(symbol, initDate, finalDate, exchange, completeOnly, exWeekends)

    symbol = 'ETHBTC'
    ETHBTC = func(symbol, initDate, finalDate, exchange, completeOnly, exWeekends)

    # store to disk
    BTCUSD.to_csv('./csv/BTCUSD.csv')
    LTCBTC.to_csv('./csv/LTCBTC.csv')
    IOTBTC.to_csv('./csv/IOTBTC.csv')
    ETHBTC.to_csv('./csv/ETHBTC.csv')

    # convert to pctdiffs
    dBTC = (BTCUSD.diff() / BTCUSD.shift()).dropna()
    dLTC = (LTCBTC.diff() / LTCBTC.shift()).dropna()
    dIOT = (IOTBTC.diff() / IOTBTC.shift()).dropna()
    dETH = (ETHBTC.diff() / ETHBTC.shift()).dropna()

    agg = pd.DataFrame([dBTC.Close, dLTC.Close, dIOT.Close, dETH.Close]).transpose()
    agg.columns = ['dBTC', 'dLTC', 'dIOT', 'dETH']

    # check correlations
    cAgg = np.corrcoef(agg.dropna(), rowvar=False)
    vAgg = np.cov(agg.dropna(), rowvar=False)

    # cut bottom 1% and top 1% of data points - prune outliers
    def middle(series, percentile):
        temp = series.sort_values(inplace=False)
        pctLen = int(round(len(temp) * percentile / 2, 0))
        temp = temp[pctLen:len(temp) - pctLen].sort_index()
        return temp

    # test for stationarity
    percentile = .02
    spreadLTC = (dLTC / dBTC).Close.dropna()
    spreadIOT = (dIOT / dBTC).Close.dropna()
    spreadETH = (dETH / dBTC).Close.dropna()

    # sBTC = adfuller(dBTC.Close)
    # sLTCBTC = adfuller(spreadLTC)
    # sIOTBTC = adfuller(spreadIOT)
    # sETHBTC = adfuller(spreadETH)

    # if stationary and correlated, check for normal distribution
    k2, p = stats.normaltest(spreadLTC)  # p <= .05

    mLTC = middle((dLTC / dBTC).Close.dropna(), percentile)
    mIOT = middle((dIOT / dBTC).Close.dropna(), percentile)
    mETH = middle((dETH / dBTC).Close.dropna(), percentile)

    sdLTC = np.std(mLTC)
    mnLTC = np.mean(mLTC)
    assdLTC = spreadLTC / sdLTC  # not using middles

    # display histogram
    spreadLTC.hist(range=[-20, 20], bins=100)
    assdLTC.hist(range=[-5, 5], bins=100)

    # sanity check
    prunedPct = len(assdLTC[np.abs(assdLTC) >= 3]) / len(assdLTC) + percentile

    # slice into sd levels and check autocorrelations
    def checkAutocorrelations(series, sdbottom, sdtop, lags):
        glomSeries = pd.DataFrame(series)
        for lag in range(1, lags + 1):
            glomSeries = glomSeries.join(pd.DataFrame(series.shift(lag)),
                                         rsuffix=str(lag), how='outer')
        subSeries = glomSeries[(np.abs(glomSeries.Close) >= sdbottom)
                             & (np.abs(glomSeries.Close) < sdtop)].dropna()
        corrs = np.corrcoef(subSeries, rowvar=False)

        mainCol = subSeries.Close

        winProps = np.empty(0)
        for col in subSeries.columns:
            winners = subSeries[(
                  ((mainCol > 0) & (mainCol > subSeries[col]))
                | ((mainCol < 0) & (mainCol < subSeries[col]))
            )]
            winProp = len(winners) / len(subSeries)
            winProps = np.append(winProps, winProp)

        return corrs[0], winProps

    # check autocorrelation
    priorSD = 0
    for thisSD in np.arange(0.25, 5.25, 0.25):
        cor, win = checkAutocorrelations(spreadLTC, priorSD, thisSD, 9)
        print(thisSD, "C", cor)
        print(thisSD, "W", win)
        priorSD = thisSD

    return


def main():
    print("Daily")
    initDate = dt.datetime(2015, 1, 1)
    finalDate = dt.datetime(2019, 1, 1)
    analyze(initDate, finalDate, gd.getDaily)

    print("Hourly")
    initDate = dt.datetime(2018, 12, 1)
    finalDate = dt.datetime(2019, 1, 1)
    analyze(initDate, finalDate, gd.getHourly)

    print("Minutely")
    initDate = dt.datetime(2018, 12, 25)
    finalDate = dt.datetime(2019, 1, 1)
    analyze(initDate, finalDate, gd.getMinutely)
    return
