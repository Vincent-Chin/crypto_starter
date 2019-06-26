from dateutil import parser
import numpy as np
import pandas as pd
import urllib3
import json
import datetime as dt
import time
import warnings
import math


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
               completeOnly=True, exWeekends=False, data_type="daily", request_hard_limit=2000,
              request_sleep_interval=.2):
    """

    :param symbol:
    :param initDate:
    :param finalDate:
    :param exchange:
    :param completeOnly:
    :param exWeekends:
    :param data_type:
    :param request_hard_limit: Hard limit imposed by cryptocompare API.
    :param request_sleep_interval: Time to wait between pulls, to prevent DDOS mishaps.
    :return:
    """

    startDate = parser.parse(str(initDate))
    endDate = parser.parse(str(finalDate))

    # figure out the difference between the two dates, use that as the limit
    limit = (endDate - startDate).total_seconds()
    baseURL = "https://min-api.cryptocompare.com/data/histoday"
    current_time = dt.datetime.now().utcnow()
    if data_type == "daily":
        # current date is incomplete in GMT, so drop it from request.
        if completeOnly and current_time.date() == endDate.date():
            endDate -= dt.timedelta(days=1)
        limit = int(max(1, limit / 86400))
    elif data_type == "hourly":
        # offset for hours
        if completeOnly and current_time.date() == endDate.date() and current_time.hour == endDate.hour:
            endDate -= dt.timedelta(hours=1)
        limit = int(max(1, limit / 3600))
        baseURL = "https://min-api.cryptocompare.com/data/histohour"
    elif data_type == "minutely":
        # offset for minutes
        if completeOnly and current_time.date() == endDate.date() and current_time.hour == endDate.hour and\
           current_time.minute == endDate.minute:
            endDate -= dt.timedelta(minutes=1)
        limit = int(max(1, limit / 60))
        baseURL = "https://min-api.cryptocompare.com/data/histominute"

    # the data hard limit is 2000, so if our request is too long, we'll need to automatically space it
    # into multiple pulls until the entire time series is requested.
    if limit > request_hard_limit:
        requests = math.ceil(limit / request_hard_limit)
        expected_time = requests * request_sleep_interval / 60 # in minutes
        message = ' '.join(["Note: Request for", symbol, "on interval", data_type,
                            "is greater than", str(request_hard_limit),
                            "data rows at", str(limit), "data rows.",
                            "This request will be staged as", str(requests),
                            "requests.  Expected pull time: ", str(expected_time), "minutes."])
        warnings.warn(message)

    info = pd.DataFrame()
    requested_end_date = endDate
    while requested_end_date > startDate:
        this_limit = int(min(limit, request_hard_limit))

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
            "limit": this_limit,
            "toTs": int(requested_end_date.timestamp())
        }

        urllib3.disable_warnings()
        http = urllib3.PoolManager(timeout=30)
        jsonString = http.request('GET', baseURL, fields=fields).data.decode()
        jsonDict = json.loads(jsonString)
        this_info = pd.DataFrame.from_dict(jsonDict['Data'])

        # abort the loop if we don't have enough info.
        if len(this_info) < 1:
            print("Reached data limit at", requested_end_date, "total data points:", len(info))
            break
        else:
            info = info.append(this_info, sort=False)
            requested_end_date = pd.to_datetime(info.sort_values(['time']).iloc[0]['time'], unit='s')
            time.sleep(request_sleep_interval)

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

    if len(info) > 0:
        info = info[info.close > 0].copy()
        info['time'] = pd.to_datetime(info.time, unit='s')
        info.index = info.time
        info.index.name = 'Timestamp'
        info['Open'] = info.open
        info['High'] = info.high
        info['Low'] = info.low
        info['Close'] = info.close
        info['Volume'] = info.volumefrom
        info = info.drop(['close', 'high', 'open', 'low',
                          'volumefrom', 'volumeto', 'time'], axis=1)
        info.sort_index(inplace=True)
        info.drop_duplicates(inplace=True)
        info = dropDirty(info, exWeekends)

    return info
#######################################################################
