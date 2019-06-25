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
# pulls any crypto data
def getCrypto(symbol, initDate, finalDate, exchange="CCCAGG",
               completeOnly=True, exWeekends=False, data_type="daily"):
    startDate = parser.parse(str(initDate))
    endDate = parser.parse(str(finalDate))

    # current date is incomplete in GMT, so drop it from request.
    if completeOnly:
        if dt.datetime.now().utcnow().date() == endDate.date():
            endDate -= dt.timedelta(days=1)

    # figure out the difference between the two dates, use that as the limit
    limit = (endDate - startDate).total_seconds()
    baseURL = "https://min-api.cryptocompare.com/data/histoday"
    if data_type == "daily":
        limit = int(max(1, limit / 86400))
    elif data_type == "hourly":
        limit = int(max(1, limit / 3600))
        baseURL = "https://min-api.cryptocompare.com/data/histohour"
    elif data_type == "minutely":
        limit = int(max(1, limit))
        baseURL = "https://min-api.cryptocompare.com/data/histominute"

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
    """
    https://bitcointalk.org/index.php?topic=1995403.0 
    "volumeto" means the volume in the currency that is being traded
    "volumefrom" means the volume in the base currency that things are traded into.

    lets say you are looking at the Doge/BTC market.
    the volumeto is the Doge volume for example 1,000,000
    the volumefrom is the BTC volume which if we assume Doge price was 100 fixed it will be 10,000 
    """

    info = info[info.close > 0].copy()
    info['time'] = pd.to_datetime(info.time, unit='s')
    info = info.dropna()
    info.index = info.time
    info.index.name = 'Timestamp'
    info['Open'] = info.open
    info['High'] = info.high
    info['Low'] = info.low
    info['Close'] = info.close
    info['Volume'] = info.volumefrom
    info = info.drop(['close', 'high', 'open', 'low',
                      'volumefrom', 'volumeto', 'time'], axis=1)

    info = dropDirty(info, exWeekends)

    return info
#######################################################################