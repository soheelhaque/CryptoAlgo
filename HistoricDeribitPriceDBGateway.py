# coding=utf-8
import ccxt
from ccxt.base.exchange import Exchange
import time
from datetime import datetime
import psycopg2
import tomli
import re
import logging
from logging.handlers import TimedRotatingFileHandler
import QuantLib as ql
import requests


OHLCV_TIMESTAMP = 3  # column of human-readable timestamp


def get_ohlcv(exchange: Exchange, market: dict) -> list:

    if exchange.has['fetchOHLCV']:
        symbol = market['symbol']

        time.sleep(exchange.rateLimit / 1000) # time.sleep wants seconds
        # time_from = 1534201200000 # Deribit starts on 14 Aug 2018
        ohlcv_page = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=5000)
        # pp.pprint(ohlv_page)
        #print(datetime.fromtimestamp(ohlv_page[0][0]/1000).strftime("%d %B %Y %H:%M:%S"))

        table = []
        for ohlcv_row in ohlcv_page:
            print("ROW", ohlcv_row)
            row = []
            row.append(exchange.id)
            row.append(symbol)
            exchange_date = datetime.fromtimestamp(ohlcv_row[0]/1000)
            row.append(exchange_date)
            row.extend(ohlcv_row)
            table.append(row)
        return table


def check_ohlcv_table_exists(cursor: psycopg2.extensions.cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS OHLCV (
                        ts TIMESTAMP,
                        Exchange  STRING NOT NULL,
                        MarketSymbol  STRING NOT NULL,
                        ExchangeDate TIMESTAMP NOT NULL,
                        ExchangeTimestamp LONG NOT NULL,
                        Open  FLOAT,
                        High  FLOAT,
                        Low   FLOAT,
                        Close FLOAT,
                        Volume  FLOAT
                ) timestamp(ts);''')

def check_ohlcv_implied_vol_table_exists(cursor: psycopg2.extensions.cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS OHLCV_IMPLIED_VOL (
                        ts TIMESTAMP,
                        Exchange  STRING NOT NULL,
                        MarketSymbol  STRING NOT NULL,
                        ExchangeDate TIMESTAMP NOT NULL,
                        ExchangeTimestamp LONG NOT NULL,
                        Open  FLOAT,
                        High  FLOAT,
                        Low   FLOAT,
                        Close FLOAT,
                        Volume  FLOAT
                ) timestamp(ts);''')


def update_ohlcv_table(connection: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor, ohlcv_table: list, last_update: int=0) -> int:
    now = datetime.utcnow()
    rowcount = 0
    for ohlcv_row in ohlcv_table:
        if ohlcv_row[OHLCV_TIMESTAMP] > last_update:
            cursor.execute('''
                INSERT INTO OHLCV
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                ''',
                (now, ohlcv_row[0], ohlcv_row[1], ohlcv_row[2], ohlcv_row[3], ohlcv_row[4], ohlcv_row[5], ohlcv_row[6], ohlcv_row[7], ohlcv_row[8]))
            rowcount += cursor.rowcount
    connection.commit()
    return rowcount


def get_ohlcv_last_update_time(cursor: psycopg2.extensions.cursor, exchange_id: str, market_symbol: str) -> int:
    cursor.execute('''SELECT max(ExchangeTimestamp)
                        FROM 'OHLCV'
                        WHERE MarketSymbol = %s
                        AND Exchange = %s;
                        ''',
                   (market_symbol, exchange_id))
    last_update_time_ms = cursor.fetchone()
    return last_update_time_ms[0]


def get_ohlcv_implied_vol_last_update_time(cursor: psycopg2.extensions.cursor, exchange_id: str) -> int:
    # print("EXCHANGE_ID", exchange_id)
    cursor.execute(f"SELECT max(ExchangeTimestamp) FROM 'OHLCV_IMPLIED_VOL' WHERE Exchange = '{exchange_id}';")
    last_update_time_ms = cursor.fetchone()
    return last_update_time_ms[0]


def load_config(db_config: dict, ccxt_markets: dict, logging_config: dict) -> None:
    with open("CryptoPriceDBGateway.toml", mode="rb") as cf:
        config = tomli.load(cf)

        # database config
        db_config['user'] = config['database']['user']
        db_config['password'] = config['database']['password']
        db_config['host'] = config['database']['host']
        db_config['port'] = config['database']['port']
        db_config['database'] = config['database']['database']

        # ccxt exchanges & markets
        ccxt_markets.update(config['ccxt'])

        # logging
        logging_config['filename'] = config['logging']['filename']
        logging_config['level'] = config['logging']['level']


def get_market_symbols(market_id_pattern: str, markets: dict) -> list:
    matching_market_ids = [market_id for market_id in markets.keys() if re.search(market_id_pattern, market_id)]
    # print("MARKET IDS", markets.keys())
    return matching_market_ids

def filter_swap_market_symbols(markets: dict) -> list:
    # Windows
    # deribit.markets = ['^BTC\/USD:BTC$','^ETH\/USD:ETH$', '^BTC\/USD:BTC-\d{6}:\d*:[CP]$', '^ETH\/USD:ETH-\d{6}:\d*:[CP]$']
    # Posix
    # deribit.markets = ['^BTC\/USD:BTC$','^ETH\/USD:ETH$', '^BTC\/USD:BTC-\d{6}-\d*-[CP]$', '^ETH\/USD:ETH-\d{6}-\d*-[CP]$']

    return [market_id for market_id in markets.keys() if ":" in market_id]


def get_ccxt_symbol(instrument: dict) -> str:

    base = instrument['base_currency']
    quote = instrument['counter_currency']
    settle = instrument['settlement_currency']

    symbol = base + '/' + quote + ':' + settle
    kind = instrument['kind']
    settlementPeriod = instrument['settlement_period']
    swap = (settlementPeriod == 'perpetual')
    future = not swap and (kind.find('future') >= 0)
    option = (kind.find('option') >= 0)
    isComboMarket = kind.find('combo') >= 0
    # if option or future:
    #     symbol = symbol + '-' + self.yymmdd(expiry, '')
    #     if option:
    #         strike = self.safe_number(market, 'strike')
    #         optionType = self.safe_string(market, 'option_type')
    #         letter = 'C' if (optionType == 'call') else 'P'
    #         symbol = symbol + '-' + self.number_to_string(strike) + '-' + letter

    return symbol

def get_historic_markets(exchange_id: str) -> dict:

    if exchange_id == 'deribit':

        session = requests.Session()



        # get currencies
        url = "https://www.deribit.com"
        action = "/api/v2/public/get_currencies"
        params = {}

        response = session.get(url + action, params=params)

        currencies = [currency['currency'] for currency in response.json()['result'] if currency['currency'] != 'ETHW']
        print("GOT CCYS", currencies)

        symbol_list = []
        url = "https://history.deribit.com"
        action = "/api/v2/public/get_instruments"
        for currency in currencies:
            params = {'currency': currency, 'include_old': 'true', 'count': 10000, 'expired': 'true', 'kind' : 'future'}

            response = session.get(url + action, params=params)
            # print(currency, len(response.json()['result']), response.json()['result'][:2])

            symbol_list.extend([get_ccxt_symbol(instrument) for instrument in response.json()['result'][:5]])

            print(currency, symbol_list)

        return {symbol: {'symbol': symbol} for symbol in symbol_list}
        #
        # result = response.json()['result']
        # # print(result[:3])
        #
        # print(len(result))
        # for symbol in result:
        #     if 'DVOL' in symbol['instrument_name']:
        #         print(symbol['instrument_name'])
        #     print(symbol['instrument_name'])
        #     # if symbol['instrument_name'] == instrument_name:
        #     #     print(symbol)

def update_markets(ccxt_markets: dict, connection, cursor) -> None:
    # exchange_ids = set(ccxt_markets.keys())
    # print("EXCH ID", ccxt_markets['exchanges'])
    exchange_ids: dict = ccxt_markets['exchanges']

    for exchange_id in exchange_ids:
        if exchange_id in ccxt.exchanges:
            exchange = eval('ccxt.%s ()' % exchange_id)  # Connect to exchange
            markets = get_historic_markets(exchange_id)  # Load all historic markets for that exchange
            logger.info('Loaded markets for exchange {}.'.format(exchange_id))
            print(markets)
            market_symbols: list = filter_swap_market_symbols(markets)
            for market_symbol in market_symbols:
                market = markets[market_symbol]
                ohlcv_table: list = get_ohlcv(exchange, market)
                last_update: int = get_ohlcv_last_update_time(cursor, exchange_id, market_symbol)
                # if last_update:
                #     rows_inserted = update_ohlcv_table(connection, cursor, ohlcv_table, last_update)
                # else:
                #     rows_inserted = update_ohlcv_table(connection, cursor, ohlcv_table)
                logger.info("{0} rows inserted for market {2} on exchange {1}.".format(rows_inserted, exchange.name,
                                                                                 market_symbol))


def get_all_new_option_data(cursor, exchange_id, last_updated=None):
    # print("EXCHANGE_ID", exchange_id)
    if last_updated is None:
        last_updated = 0

    cursor.execute(f"SELECT * FROM 'OHLCV' WHERE ExchangeTimestamp > {last_updated} AND Exchange = '{exchange_id}' AND RIGHT(MarketSymbol,2) IN ('-C', '-P');")
    options = cursor.fetchall()
    print("OPTIONS", len(options))
    return options


def get_underlying_future(cursor, exchange_id, symbol, exchange_time):
    # print("EXCHANGE_ID", exchange_id)):

    cursor.execute(
        f"SELECT * FROM 'OHLCV' WHERE ExchangeTimestamp = {exchange_time} AND Exchange = '{exchange_id}' AND MarketSymbol = '{symbol}';")
    future = cursor.fetchone()
    # print("FUTURE", future)
    return future


def calc_implied_vol(strike, underlying, calculation_date, expiry_date, mark_price, call_put, risk_free_rate=0.0):

    # print(calculation_date, expiry_date, strike, underlying, mark_price, call_put)

    option_type = ql.Option.Call
    volatility = 1

    if call_put == "P":
        option_type = ql.Option.Put

    _MONTH_MAP = {"JAN": 1, "FEB": 2, 'MAR': 3, 'APR': 4,
                  "MAY": 5, "JUN": 6, 'JUL': 7, 'AUG': 8,
                  "SEP": 9, "OCT": 10, 'NOV': 11, 'DEC': 12
                  }

    def _date_split(calculation_date: str):
        """ Utility for splitting the date string into parts """

        year = int(calculation_date[:4])
        month = int(calculation_date[5:7])
        day = int(calculation_date[-2:])
        return day, month, year

    today = ql.Date(*_date_split(calculation_date))

    ql.Settings.instance().evaluationDate = today

    expiry = ql.Date(*_date_split(expiry_date))

    # The Instrument
    option = ql.EuropeanOption(ql.PlainVanillaPayoff(option_type, strike),
                               ql.EuropeanExercise(expiry))
    # The Market
    u = ql.SimpleQuote(underlying)  # set todays value of the underlying
    r = ql.SimpleQuote(risk_free_rate / 100)  # set risk-free rate
    sigma = ql.SimpleQuote(volatility / 100)  # set volatility
    riskFreeCurve = ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(r), ql.Actual360())
    volatilityCurve = ql.BlackConstantVol(0, ql.TARGET(), ql.QuoteHandle(sigma), ql.Actual365Fixed())
    # The Model
    process = ql.BlackProcess(ql.QuoteHandle(u),
                              ql.YieldTermStructureHandle(riskFreeCurve),
                              ql.BlackVolTermStructureHandle(volatilityCurve))

    try:
        return option.impliedVolatility(mark_price * underlying, process)
    except RuntimeError:
        return None



def imply_volatilities(cursor, exchange_id, new_options):

    found, not_found, bad_vol = 1, 1, 1

    for i, option in enumerate(new_options):
        exchange_time = option[4]

        calculation_date = str(option[3].date())

        symbol = option[2]
        parts = symbol.split('-')
        token = parts[0]
        expiry_date = f'20{parts[1][:2]}-{parts[1][2:4]}-{parts[1][-2:]}'
        strike = float(parts[2])
        option_type = parts[3]

        # if symbol == 'BTC/USD:BTC-230331-20000-C':
        # if parts[1] == '230331':
        future_name = f'{token}-{parts[1]}'

        future = get_underlying_future(cursor, exchange_id, future_name, exchange_time)

        # if future is None:
        if future:
            future_open = future[5]
            future_close = future[8]
            mark_open = option[5]
            # print("FUTURE", future_open, future_close)
            found += 1
            open_vol = calc_implied_vol(strike, future_open, calculation_date, expiry_date, mark_open, option_type)
            if open_vol is None:
                bad_vol += 1
            # else:
            #     print(symbol, "VOL", open_vol * 100)
        else:
            not_found += 1

        if i % 1000 == 0:
            print(i, f'{found=}, {not_found=}, {bad_vol=}, {int(found/(found + not_found) * 100)}, {int(bad_vol/found * 100)}')


    print(f'{found=}, {not_found=}, {bad_vol=}')
        #
        # if i>60:
        #     break

def save_implied_vols(cursor, exchange_id, implied_vol_ohlcv):
    pass


def update_implied_vol(ccxt_markets: dict, connection, cursor) -> None:

    exchange_ids: dict = ccxt_markets['exchanges']

    for exchange_id in exchange_ids:

        last_update: int = get_ohlcv_implied_vol_last_update_time(cursor, exchange_id)
        # print("LAST UPDATED", last_update)
        new_options = get_all_new_option_data(cursor, exchange_id, last_update)

        if len(new_options):
            implied_vol_ohlcv = imply_volatilities(cursor, exchange_id, new_options)

            save_implied_vols(cursor, exchange_id, implied_vol_ohlcv)


def set_up_logger(config: dict) -> logging.Logger:
    # logger
    logger = logging.getLogger()
    logger.setLevel(logging.getLevelName(config['level']))

    # file handler
    fh = TimedRotatingFileHandler(config['filename'], when="w0", interval=1, backupCount=5)
    fh.setLevel(logging.getLevelName(config['level']))

    # console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.getLevelName(config['level']))

    # formatter
    formatter = logging.Formatter(fmt="%(asctime)s - %(message)s", datefmt="%d-%b-%y %H:%M:%S")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def process_ohlcv_price(db_cursor, db_connection, markets):
    # create table if needed, then update with 'new' records in the timeseries
    check_ohlcv_table_exists(db_cursor)
    update_markets(markets, db_connection, db_cursor)

def process_ohlcv_implied_vol(db_cursor, db_connection, markets):
    # create table if needed, then update with 'new' records in the timeseries
    check_ohlcv_implied_vol_table_exists(db_cursor)
    update_implied_vol(markets, db_connection, db_cursor)


if __name__ == "__main__":

    db_config: dict = {}
    markets: dict = {}
    logging_config: dict = {}

    load_config(db_config, markets, logging_config)

    logger: logging.Logger = set_up_logger(logging_config)

    try:
        # Connect to DB
        db_connection = psycopg2.connect(user=db_config['user'], password=db_config['password'], host=db_config['host'], port=db_config['port'], database=db_config['database'])
        logger.info('Postgres connection is established.')
    except Exception as e:
        logger.exception(f"An exception has occurred: {e}")
        raise e

    try:
        # open connection for read/write
        db_cursor = db_connection.cursor()
        logger.info('Postgres connection is opened.')
    except Exception as e:
        logger.exception(f"An exception has occurred: {e}")
        if db_connection:
            db_connection.close()
        logger.info('Postgres connection is closed.')
        raise e

    try:
        process_ohlcv_price(db_cursor, db_connection, markets)
        # process_ohlcv_implied_vol(db_cursor, db_connection, markets)
    except Exception as e:
        logger.exception(f"An exception has occurred: {e}")
    finally:
        if db_cursor:
            db_cursor.close()
        if db_connection:
            db_connection.close()
        logger.info('Postgres connection is closed.')
