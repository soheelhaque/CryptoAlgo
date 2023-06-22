import ccxt
from ccxt.base.exchange import Exchange

exchange_ids: list = ['deribit', 'binance']

for exchange_id in exchange_ids:
    print("PROCESS EXCHANGE", exchange_id)
    if exchange_id in ccxt.exchanges:
        exchange = eval('ccxt.%s ()' % exchange_id)  # Connect to exchange
        markets = exchange.load_markets()
        print("MARKETS", markets.keys())