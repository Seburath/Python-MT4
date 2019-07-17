from poloniex import Poloniex
polo = Poloniex()
help(polo)

#Public APIs
from poloniex import Poloniex
polo = Poloniex()
ticker = polo.returnTicker()['BTC_ETH']
print(ticker)

#Private APIs
from poloniex import Poloniex
import os

api_key = os.environ.get('POLONIEX_API_KEY')
api_secret = os.environ.get('POLONIEX_SECRET')
polo = Poloniex(api_key, api_secret)

ticker = polo.returnTicker()['BTC_ETH']
print(ticker)

balances = polo.returnBalances()
print(balances)
