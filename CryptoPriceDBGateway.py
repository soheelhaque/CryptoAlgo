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


OHLCV_TIMESTAMP = 3  # column of human-readable timestamp


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


def update_markets(ccxt_markets: dict, connection, cursor) -> None:
    # exchange_ids = set(ccxt_markets.keys())
    # print("EXCH ID", ccxt_markets['exchanges'])
    exchange_ids: dict = ccxt_markets['exchanges']

    for exchange_id in exchange_ids:
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
                logger.info("{0} rows inserted for market {2} on exchange {1}.".format(rows_inserted, exchange.name,
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


def process_ohlcv_price(db_cursor, db_connection, markets):
    # create table if needed, then update with 'new' records in the timeseries
    check_ohlcv_table_exists(db_cursor)
    update_markets(markets, db_connection, db_cursor)


if __name__ == "__main__":

    db_config: dict = {}
    markets: dict = {}
    logging_config: dict = {}

    load_config(db_config, markets, logging_config)

    markets = {'exchanges': ['binance']}

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
