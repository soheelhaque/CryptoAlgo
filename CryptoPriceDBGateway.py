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


OHLCV_TIMESTAMP = 3  # column of human-readable timestamp
OHLCV_VOL_TABLE = 'OHLCV_VOL'

def get_ohlcv(exchange: Exchange, market: dict) -> list:

    if exchange.has['fetchOHLCV']:
        symbol = market['symbol']
        if symbol in exchange.markets:
            time.sleep(exchange.rateLimit / 1000) # time.sleep wants seconds
            # time_from = 1534201200000 # Deribit starts on 14 Aug 2018
            ohlcv_page = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=5000)
            # pp.pprint(ohlv_page)
            #print(datetime.fromtimestamp(ohlv_page[0][0]/1000).strftime("%d %B %Y %H:%M:%S"))

            table = []
            for ohlcv_row in ohlcv_page:
                # print(ohlcv_row)
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


def check_ohlcv_vol_table_exists(cursor: psycopg2.extensions.cursor):
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {OHLCV_VOL_TABLE} (
                        ts TIMESTAMP,
                        Exchange  STRING NOT NULL,
                        MarketSymbol  STRING NOT NULL,
                        ExchangeDate TIMESTAMP NOT NULL,
                        ExchangeTimestamp LONG NOT NULL,
                        OpenVol  FLOAT,
                        HighVol  FLOAT,
                        LowVol   FLOAT,
                        CloseVol FLOAT,
                        OpenStrike  FLOAT,
                        HighStrike  FLOAT,
                        LowStrike   FLOAT,
                        CloseStrike FLOAT,
                        Term FLOAT,
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


def find_option_underlying(ohlcv_row: list, ohlcv_futures_table: list) -> list:

    result = []
    exchange_date = ohlcv_row[2]
    for future in ohlcv_futures_table:
        if future[2] == exchange_date:
            return future

    return result


def _calc_implied_vol(strike, underlying, calculation_date: datetime, expiry_date, mark_price, call_put, risk_free_rate=0.0):

    # print("CALC VOL", calculation_date, expiry_date, strike, underlying, mark_price, call_put)

    option_type = ql.Option.Call
    if call_put == "P":
        option_type = ql.Option.Put

    def _date_split(calculation_date: str):
        """ Utility for splitting the date string into parts """
        # print("DATE", calculation_date)
        year = int("20" + calculation_date[:2])
        month = int(calculation_date[2:4])
        day = int(calculation_date[-2:])
        return day, month, year

    today = ql.Date(calculation_date.day, calculation_date.month, calculation_date.year)

    ql.Settings.instance().evaluationDate = today

    expiry = ql.Date(*_date_split(expiry_date))

    # The Instrument
    option = ql.EuropeanOption(ql.PlainVanillaPayoff(option_type, strike),
                               ql.EuropeanExercise(expiry))
    # The Market
    u = ql.SimpleQuote(underlying)  # set todays value of the underlying
    r = ql.SimpleQuote(risk_free_rate / 100)  # set risk-free rate
    sigma = ql.SimpleQuote(0.5)  # set volatility
    riskFreeCurve = ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(r), ql.Actual360())
    volatilityCurve = ql.BlackConstantVol(0, ql.TARGET(), ql.QuoteHandle(sigma), ql.Actual365Fixed())
    # The Model
    process = ql.BlackProcess(ql.QuoteHandle(u),
                              ql.YieldTermStructureHandle(riskFreeCurve),
                              ql.BlackVolTermStructureHandle(volatilityCurve))

    try:
        return option.impliedVolatility(mark_price * underlying, process) * 100
    except RuntimeError as e:

        if 'root not' in str(e):
            return 400
        if 'expired' in str(e):
            return 0
        print("ERROR", e)
        return None


def calc_implied_option_vol(option: list, future: list) -> dict:

    # ohlcv_row['symbol'], ohlcv_row['exchange_date'], ohlcv_row['timestamp'],
    # ohlcv_row['open_vol'], ohlcv_row['high_vol'], ohlcv_row['low_vol'], ohlcv_row['close_vol'],
    # ohlcv_row['open_strike'], ohlcv_row['high_strike'], ohlcv_row['low_strike'], ohlcv_row['close_strike'],
    # ohlcv_row['term'], ohlcv_row['volume']))



    ohlcv_row = {}
    ohlcv_row['symbol'] = option[1]
    ohlcv_row['exchange_date'] = option[2]
    ohlcv_row['timestamp'] = option[3]
    ohlcv_row['volume'] = option[8]

    option_data = option[1].split('-')
    strike = float(option_data[2])
    call_put = option_data[3]
    expiry = option_data[1]
    calculation_date = option[2]

    date_exp = datetime.strptime(expiry, '%y%m%d')
    delta = date_exp - calculation_date
    ohlcv_row['term'] = delta.days

    prices = {'open': 4, 'high': 5, 'low': 6, 'close': 7}

    for price, i in prices.items():
        underlying = future[i]
        mark_price = option[i]
        vol_price = _calc_implied_vol(strike, underlying, calculation_date, expiry, mark_price, call_put)

        if vol_price is None:
            print("GOT BAD VOL WITH ERROR")
            return {}
        #
        # print("GOT IMPLIED VOLS", option[1], price, "vol", vol_price, "strike", strike, "und", underlying, "calc date", calculation_date, "expiry", expiry, "option value", mark_price)

        ohlcv_row[price + "_vol"] = vol_price
        ohlcv_row[price + '_strike'] = strike / underlying * 100

    return ohlcv_row


def update_ohlcv_vol_row(connection: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor, option_row: list, underlying_row: list) -> int:

    ohlcv_row = calc_implied_option_vol(option_row, underlying_row)

    if not ohlcv_row:
        # print("BAD DATA")
        return 0

    # print("GOT VOLS", ohlcv_row)

    now = datetime.utcnow()
    cursor.execute(f'''
                        INSERT INTO {OHLCV_VOL_TABLE}
                        VALUES(%s, %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s);
                        ''',
                               (now, 'deribit', ohlcv_row['symbol'], ohlcv_row['exchange_date'], ohlcv_row['timestamp'],
                                ohlcv_row['open_vol'], ohlcv_row['high_vol'], ohlcv_row['low_vol'], ohlcv_row['close_vol'],
                                ohlcv_row['open_strike'], ohlcv_row['high_strike'], ohlcv_row['low_strike'], ohlcv_row['close_strike'],
                                ohlcv_row['term'], ohlcv_row['volume']))

    return 1

def update_ohlcv_vol_table(connection: psycopg2.extensions.connection, cursor: psycopg2.extensions.cursor, ohlcv_option_table: list, ohlcv_future_table: list, last_update: int=0) -> int:

    if last_update is None:
        last_update = 0

    rowcount = 0
    for option_row in ohlcv_option_table:
        if option_row[OHLCV_TIMESTAMP] > last_update:

            # print("INSERT", option_row)
            underlying_row = find_option_underlying(option_row, ohlcv_future_table)
            if underlying_row:
                # print('USING', underlying_row)
                rowcount += update_ohlcv_vol_row(connection, cursor, option_row, underlying_row)

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

def get_ohlcv_vol_last_update_time(cursor: psycopg2.extensions.cursor, exchange_id: str, market_symbol: str) -> int:
    cursor.execute(f'''SELECT max(ExchangeTimestamp)
                        FROM {OHLCV_VOL_TABLE}
                        WHERE MarketSymbol = %s
                        AND Exchange = %s;
                        ''',
                   (market_symbol, exchange_id))
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

    return [market_id for market_id in markets.keys() if ":" in market_id]


def is_option(symbol: str) -> bool:

    if symbol.endswith('-C') or symbol.endswith('-P'):
        return True

    return False

def update_markets(ccxt_markets: dict, connection, cursor) -> None:
    # exchange_ids = set(ccxt_markets.keys())
    # print("EXCH ID", ccxt_markets['exchanges'])
    exchange_ids: dict = ccxt_markets['exchanges']

    for exchange_id in exchange_ids:
        print("PROCESS EXCHANGE", exchange_id)
        if exchange_id in ccxt.exchanges:
            exchange = eval('ccxt.%s ()' % exchange_id)  # Connect to exchange
            markets = exchange.load_markets()  # Load all markets for that exchange
            # print(exchange_id)
            # print(markets.keys())
            # print(markets['AVAX/USDC:USDC'])
            # break
            logger.info('Loaded markets for exchange {}.'.format(exchange_id))

            market_symbols: list = filter_swap_market_symbols(markets)
            for market_symbol in market_symbols:
                market = markets[market_symbol]
                ohlcv_table: list = get_ohlcv(exchange, market)
                last_update: int = get_ohlcv_last_update_time(cursor, exchange_id, market_symbol)
                if last_update:
                    rows_inserted = update_ohlcv_table(connection, cursor, ohlcv_table, last_update)
                else:
                    rows_inserted = update_ohlcv_table(connection, cursor, ohlcv_table)

                logger.info("{0} price rows inserted for market {2} on exchange {1}.".format(rows_inserted, exchange.name,
                                                                                 market_symbol))

            # do vols
            if exchange_id == 'deribit':
                for market_symbol in market_symbols:
                    if is_option(market_symbol):
                        future_symbol = market_symbol.split('-')[0] + "-" + market_symbol.split('-')[1]
                        market = markets[market_symbol]
                        try:
                            future = markets[future_symbol]
                        except KeyError:
                            # print("Skipping", market_symbol, future_symbol)
                            continue
                        ohlcv_option_table: list = get_ohlcv(exchange, market)
                        if not ohlcv_option_table:
                            # print("skipping - no option prices")
                            continue
                        ohlcv_future_table: list = get_ohlcv(exchange, future)
                        last_vol_update: int = get_ohlcv_vol_last_update_time(cursor, exchange_id, market_symbol)
                        # print("LAST UPDATE", market_symbol, len(ohlcv_option_table), last_vol_update, future_symbol, len(ohlcv_future_table))
                        # print("OPTION TABLE", ohlcv_option_table)
                        # print("FUTURE TABLE", ohlcv_future_table)
                        rows_inserted = update_ohlcv_vol_table(connection, cursor, ohlcv_option_table, ohlcv_future_table, last_vol_update)
                        logger.info("{0} implied vol rows inserted for market {2} on exchange {1}.".format(rows_inserted, exchange_id,
                                                                                         market_symbol))


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

def update_vols(db_connection, db_cursor):

    # iterate through all option price data for deribit

    # if vol record does not already exist for it

        # process_instrument_vol

    pass



def process_ohlcv_price(db_cursor, db_connection, markets):
    # create table if needed, then update with 'new' records in the timeseries
    check_ohlcv_table_exists(db_cursor)
    check_ohlcv_vol_table_exists(db_cursor)
    update_markets(markets, db_connection, db_cursor)


if __name__ == "__main__":

    db_config: dict = {}
    markets: dict = {}
    logging_config: dict = {}

    load_config(db_config, markets, logging_config)

    # markets = {'exchanges': ['binance']}

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
    except Exception as e:
        logger.exception(f"An exception has occurred: {e}")
    finally:
        if db_cursor:
            db_cursor.close()
        if db_connection:
            db_connection.close()
        logger.info('Postgres connection is closed.')
