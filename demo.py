import datetime as dt

import get_data as gd

def main():

    initDate = dt.datetime(2015, 1, 1)
    finalDate = dt.datetime(2019, 1, 1)

    # aggregated daily price for Bitcoin (2000 row limit - use a loop)
    symbol = 'BTCUSD'
    exchange = 'CCCAGG'
    completeOnly = True
    exWeekends = False
    dailyData = gd.getDaily(symbol, initDate, finalDate, exchange, completeOnly,
                            exWeekends)

    # hourly GDAX price for Litecoin (2000 row limit - use a loop)
    symbol = 'LTCUSD'
    exchange = 'GDAX'
    completeOnly = True
    exWeekends = False
    hourlyData = gd.getHourly(symbol, initDate, finalDate, exchange, completeOnly,
                            exWeekends)

    # minutely Binance price for IOTA (2000 row limit - use a loop)
    symbol = 'IOTBTC'
    exchange = 'Binance'
    completeOnly = True
    exWeekends = False
    minutelyData = gd.getMinutely(symbol, initDate, finalDate, exchange, completeOnly,
                            exWeekends)

    # dump to csv
    dailyData.to_csv('./csv/days.csv')
    hourlyData.to_csv('./csv/hours.csv')
    minutelyData.to_csv('./csv/minutes.csv')

    return