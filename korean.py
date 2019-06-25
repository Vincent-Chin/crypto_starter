import datetime as dt
import get_data as gd
import pandas as pd
import time

def main():

    exchanges = ['Binance', 'GDAX', 'Bithumb', 'Korbit']
    symbols = ['BTCUSD', 'BTC.USDT', 'BTCKRW',
               'ETHUSD', 'ETH.USDT', 'ETHKRW',
               'XRPUSD', 'XRP.USDT', 'XRPKRW']

    initDate = dt.datetime(2018, 1, 1)
    finalDate = dt.datetime(2019, 1, 1)
    completeOnly = True
    exWeekends = False

    aggData = pd.DataFrame()
    for symbol in symbols:
        for exchange in exchanges:
            try:
                data = gd.getCrypto(symbol, initDate, finalDate, exchange, completeOnly, exWeekends, data_type="daily")
                data['Symbol'] = symbol
                data['Exchange'] = exchange
                data['Timestamp'] = data.index
                aggData = aggData.append(data, ignore_index=True)
                print(dt.datetime.now(), "Got", symbol, exchange)
                time.sleep(0.1)
            except:
                pass

    # dump to csv
    aggData.sort_values(['Timestamp', 'Symbol', 'Exchange'])
    aggData.index = aggData.Timestamp

    aggData.to_csv('./csv/aggData.csv')

    # just play with tail
    aggTail = aggData["2018-01-08"].copy()
    usdkrw = 1072.71

    aggTail['KRWEq'] = aggTail.Close * usdkrw
    aggTail['USDEq'] = aggTail.Close / usdkrw

    usExg = 'Binance'
    koExg = 'Korbit'

    BTCSpread = (aggTail[(aggTail.Symbol == 'BTCKRW') & (aggTail.Exchange == koExg)].USDEq
        / aggTail[(aggTail.Symbol == 'BTC.USDT') & (aggTail.Exchange == usExg)].Close)

    ETHSpread = (aggTail[(aggTail.Symbol == 'ETHKRW') & (aggTail.Exchange == koExg)].USDEq
        / aggTail[(aggTail.Symbol == 'ETH.USDT') & (aggTail.Exchange == usExg)].Close)

    # difference in dollars per dollar utilized
    gainPerDollar = abs(BTCSpread - ETHSpread)
    txnfees = .001 * 2
    BTCRate = aggTail[(aggTail.Symbol == 'BTC.USDT') & (aggTail.Exchange == usExg)].Close
    withdrawfee = .001 * 2 * 1 / BTCRate

    netGain = gainPerDollar - txnfees - withdrawfee
    gainPerBTC = netGain * BTCRate

    print("Net Gain", netGain)
    print("Gain Per BTC", gainPerBTC)


    return
