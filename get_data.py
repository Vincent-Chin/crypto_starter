from dateutil import parser
import numpy as np
import pandas as pd
import urllib3
import json
import datetime as dt
import time

#######################################################################
# drops invalid data from our history
def dropDirty(history, exWeekends):
    history = history[(history.Open != 0)
                    & (history.High != 0)
                    & (history.Low != 0)
                    & (history.Close != 0)]

    history = history[(pd.isnull(history.Open) == False)
                    & (pd.isnull(history.High) == False)
                    & (pd.isnull(history.Low) == False)
                    & (pd.isnull(history.Close) == False)]

    # we're going to drop any days where the open and high and low and close
    # equal one another.  these are invalid (closed) days
    history = history[((history.Open == history.Close)
                      & (history.Open == history.High)
                      & (history.Open == history.Low)) == False]

    if exWeekends:
        dts = pd.to_datetime(history.index).weekday
        history = history[dts < 5] # cut out Saturday[5] and Sunday[6]

    return history
#######################################################################

#######################################################################
# pulls daily crypto data
# internal; dont use directly
def _getCrypto(baseURL, symbol, initDate, finalDate, exchange="CCCAGG",
              completeOnly=True, exWeekends=False):
    startDate = parser.parse(str(initDate))
    endDate = parser.parse(str(finalDate))
    # current date is incomplete in GMT, so drop it from request.
    if completeOnly:
        endDate -= dt.timedelta(days=1)

    # figure out the day difference between the two dates,
    # use that as the limit
    limit = (endDate - startDate).days

    # https://www.cryptocompare.com/api/#-api-data-histoday-

    # split the symbol
    if len(symbol) == 6:
        term = symbol[0:3]
        base = symbol[3:6]
    else:
        asSplit = symbol.split('.')
        if len(asSplit) == 2:
            term = asSplit[0]
            base = asSplit[1]
        else:
            asSplit = symbol.split('_')
            term = asSplit[0]
            base = asSplit[1]

    fields = {
        "fsym": term,
        "tsym": base,
        "e": exchange,
        "limit": limit,
        "toTs": int(endDate.timestamp())
    }

    urllib3.disable_warnings()
    http = urllib3.PoolManager(timeout=30)
    jsonString = http.request('GET', baseURL, fields=fields).data.decode()
    jsonDict = json.loads(jsonString)
    info = pd.DataFrame.from_dict(jsonDict['Data'])

    # rename and reindex for our internal compatibility
    # drops timestamp component after setting it as an index
    info = info[info.close > 0].copy()
    info['time'] = pd.to_datetime(info.time, unit='s')
    info = info.dropna()
    info.index = info.time
    info.index.name = 'Timestamp'
    info['Open'] = info.open
    info['High'] = info.high
    info['Low'] = info.low
    info['Close'] = info.close
    info['Volume'] = info.volumefrom.astype(int) # drop off less than whole unit
    info = info.drop(['close', 'high', 'open', 'low',
                      'volumefrom', 'volumeto', 'time'], axis=1)

    info = dropDirty(info, exWeekends)

    return info
#######################################################################

#######################################################################
# pulls daily crypto data
# Daily: https://www.cryptocompare.com/api/#-api-data-histoday-
def getDaily(symbol, initDate, finalDate, exchange="CCCAGG",
              completeOnly=True, exWeekends=False):

    baseURL = "https://min-api.cryptocompare.com/data/histoday"

    return _getCrypto(baseURL, symbol, initDate, finalDate, exchange,
                      completeOnly, exWeekends)
#######################################################################

#######################################################################
# pulls hourly crypto data
# Hourly (3 months): https://www.cryptocompare.com/api/#-api-data-histohour-
def getHourly(symbol, initDate, finalDate, exchange="CCCAGG",
             completeOnly=True, exWeekends=False):
    baseURL = "https://min-api.cryptocompare.com/data/histohour"

    return _getCrypto(baseURL, symbol, initDate, finalDate, exchange,
                      completeOnly, exWeekends)
#######################################################################

#######################################################################
# pulls minutely crypto data
# Minutely (7 days): https://www.cryptocompare.com/api/#-api-data-histominute-
def getMinutely(symbol, initDate, finalDate, exchange="CCCAGG",
             completeOnly=True, exWeekends=False):
    baseURL = "https://min-api.cryptocompare.com/data/histominute"

    return _getCrypto(baseURL, symbol, initDate, finalDate, exchange,
                      completeOnly, exWeekends)
#######################################################################