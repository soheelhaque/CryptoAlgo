import time
import tomli
import requests
from datetime import datetime
import psycopg2
import QuantLib as ql


class DeribitHistory:

    def __init__(self):

        self.session = requests.Session()
        self.history_url = "https://history.deribit.com"
        self.live_url = "https://www.deribit.com"
        self.deribit_ohlcv = "OHLCV"
        self.deribit_ohlcv_vol = "OHLCV_VOL"

        self.db_config: dict = self._load_config()
        self.db_cursor = None
        self.db_connection = None
        self._connectDB(self.db_config)

    def _connectDB(self, db_config):

        try:
            # Connect to DB
            self.db_connection = psycopg2.connect(user=db_config['user'],
                                             password=db_config['password'],
                                             host=db_config['host'],
                                             port=db_config['port'],
                                             database=db_config['database']
                                             )
            print('Postgres connection is established.')
        except Exception as e:
            print(f"An exception has occurred: {e}")
            raise e

        try:
            # open connection for read/write
            self.db_cursor = self.db_connection.cursor()
            print('Postgres connection is opened.')
        except Exception as e:
            print(f"An exception has occurred: {e}")
            if self.db_connection:
                self.db_connection.close()
            print('Postgres connection is closed.')
            raise e

    def _load_config(self) -> dict:

        db_config = {}

        with open("DeribitHistoryDBGateway.toml", mode="rb") as cf:
            config = tomli.load(cf)

            # database config
            db_config['user'] = config['database']['user']
            db_config['password'] = config['database']['password']
            db_config['host'] = config['database']['host']
            db_config['port'] = config['database']['port']
            db_config['database'] = config['database']['database']

        return db_config

    def _get_tokens(self) -> list:

        return ['ETH', 'BTC']

        # return ['ETH']

    def _yymmdd(self, expiry) -> str:

        date = datetime.fromtimestamp(expiry / 1000)

        return date.strftime('%y%m%d')

    def _get_ccxt_historic_market(self, instrument: dict) -> dict:

        # print("Instrument", instrument)
        kind = instrument['kind']

        base = instrument['base_currency']
        quote = instrument['counter_currency']
        settle = instrument['settlement_currency']
        instrument_name = instrument['instrument_name']

        settlementPeriod = instrument['settlement_period']
        swap = (settlementPeriod == 'perpetual')

        isSpot = (kind == 'spot')
        isComboMarket = kind.find('combo') >= 0

        if isComboMarket or isSpot or swap:
            result = {
                'symbol': instrument_name,
                'instrument_name': instrument_name,
                'expiry': None,
                'kind': kind,
                'get_history': False
            }

            return result

        expiry = self._yymmdd(instrument['expiration_timestamp'])
        expiry_timestamp = instrument['expiration_timestamp']
        future = (kind.find('future') >= 0)
        option = (kind.find('option') >= 0)

        if not (future or option):
            raise ValueError("ERROR UNKNOWN HISTORIC INSTRUMENT TYPE FOR", instrument_name)

        symbol = base + '/' + quote + ':' + settle + '-' + expiry

        if option:
            strike = str(int(instrument['strike']))
            optionType = instrument['option_type']
            letter = 'C' if (optionType == 'call') else 'P'
            symbol = symbol + '-' + strike + '-' + letter

        result = {
            'symbol': symbol,
            'instrument_name': instrument_name,
            'expiry': expiry,
            'expiry_timestamp': expiry_timestamp,
            'kind': kind,
            'get_history': True,
        }

        return result

    def _get_ccy_markets(self, currency) -> list[dict]:

        action = "/api/v2/public/get_instruments"
        params = {'currency': currency, 'include_old': 'true', 'count': 10000, 'expired': 'true'}

        response = self.session.get(self.history_url + action, params=params)
        # for instrument in response.json()['result']:
        #     if 'option' not in instrument['kind']:
        #         print("RAW INSTRUMENTS", instrument['kind'], instrument['instrument_name'])
        return [self._get_ccxt_historic_market(instrument) for instrument in response.json()['result']]

    def _get_ohlcv_day_data(self, market: dict) -> dict:

        # print("GET OHLCV", market)

        if not market['get_history']:
            return {}

        # rate limit
        time.sleep(0.025)

        expiry_timestamp = market['expiry_timestamp']
        if expiry_timestamp is None:
            expiry_timestamp = datetime.utcnow().timestamp()
            print("EXPIRY NOW", expiry_timestamp)

        # expiry_timestamp = 1681804379000

        action = "/api/v2/public/get_tradingview_chart_data"
        params = {'instrument_name': market['instrument_name'],
                  'include_old': 'true',
                  'start_timestamp': 0,
                  'end_timestamp': expiry_timestamp,
                  'resolution': '1D'
                  }
        response = self.session.get(self.history_url + action, params=params)

        # print("RESPONSE", market['instrument_name'], response.json())

        if response.status_code != 200:
            print(response)
            raise RuntimeError(response)
        # response = session.get(url + action + '?currency=BTC&include_old=true&count=10000&kind=option&expired=true')
        if 'result' not in response.json():
            print("ERR", response)
            return {}

        if response.json()['result']['status'] != 'ok':
            return {}

        return response.json()['result']


    def _get_tick_implied_vols(self, option_name, tick, price_data):
        # print("GET IMPLIED VOLS", option_name, tick, price_data)

        option = price_data['option']
        future = price_data['future']

        calculation_date = option['calculation_date']
        strike = option['strike']
        expiry = option['expiry']
        call_put = option['option_type']

        prices = ['open', 'high', 'low', 'close']

        for price in prices:
            underlying = strike / future[price]
            mark_price = option[price]
            vol_price = self._calc_implied_vol(strike, underlying, calculation_date, expiry, mark_price, call_put)
            # print("GOT IMPLIED VOLS", option_name, price, vol_price, strike, underlying, calculation_date, expiry, mark_price, call_put)

            option[price + "_vol"] = vol_price

    def _calc_implied_vol(self, strike, underlying, calculation_date, expiry_date, mark_price, call_put, risk_free_rate=0.0):

        # print(calculation_date, expiry_date, strike, underlying, mark_price, call_put)

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

        today = ql.Date(*_date_split(calculation_date))

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

    # def _check_if_exists(self, ohlcv_data):
    #
    #     market_symbol = ohlcv_data['MarketSymbol']
    #     exchange = ohlcv_data['Exchange']
    #     exchange_timestamp = ohlcv_data['ExchangeTimestamp']
    #
    #     self.db_cursor.execute(f'''SELECT *
    #                             FROM '{self.deribit_ohlcv_vol}'
    #                             WHERE MarketSymbol = %s
    #                             AND Exchange = %s
    #                             AND ExchangeTimestamp = %s;
    #                             ''',
    #                    (market_symbol, exchange, exchange_timestamp))
    #     record = self.db_cursor.fetchone()
    #     print(market_symbol, record, ohlcv_data)

    # def _make_ohlcv_table_row(self, market, i, history):
    #
    #     return {
    #         'Exchange': 'deribit',
    #         'MarketSymbol': market['symbol'],
    #         'ExchangeDate': datetime.fromtimestamp(history['ticks'][i]/1000),
    #         'ExchangeTimestamp': history['ticks'][i],
    #         'Open': history['open'][i],
    #         'High': history['high'][i],
    #         'Low': history['low'][i],
    #         'Close': history['close'][i],
    #         'Volume': history['volume'][i],
    #
    #     }

    def _get_option_and_future_instruments(self):

        result = {'futures': [], 'options': []}

        for token in self._get_tokens():

            token_markets = self._get_ccy_markets(token)
            # for instrument in token_markets:
            #     if 'option' not in instrument['kind']:
            #         print("RAW INSTRUMENTS", instrument['kind'], instrument['instrument_name'])
            token_futures = [market for market in token_markets if market['kind'] == 'future']
            # print("TOKEN FUTURES", token_futures)
            token_options = [market for market in token_markets if market['kind'] == 'option']

            result['futures'] += token_futures
            result['options'] += token_options

        return result

    def _transform_to_date(self, ohlcv_history):

        result = {}

        for i, tick in enumerate(ohlcv_history['ticks']):

            result[tick] = {'volume': ohlcv_history['volume'][i],
                             'open': ohlcv_history['open'][i],
                             'high': ohlcv_history['high'][i],
                             'low': ohlcv_history['low'][i],
                             'close': ohlcv_history['close'][i],
                             'cost': ohlcv_history['cost'][i],
                            }

        return result

    def _get_option_and_future_prices(self, historic_instruments, period) -> dict:

        historic_option_prices = {}
        historic_future_prices = {}

        for future in historic_instruments['futures']:

            if future['symbol'].split('-')[1].startswith(period):
                print("FUTURE HISTORY", future['symbol'])
                time.sleep(0.02)

                future_history = self._get_ohlcv_day_data(future)
                # print(future['symbol'], len(future_history))
                if future_history:
                    historic_future_prices[future['symbol']] = self._transform_to_date(future_history)

        for option in historic_instruments['options']:

            if option['symbol'].split('-')[1].startswith(period):
                print("OPTION HISTORY", option['symbol'])
                time.sleep(0.02)
                option_history = self._get_ohlcv_day_data(option)
                # print(option['symbol'], len(option_history))
                if option_history:
                    historic_option_prices[option['symbol']] = self._transform_to_date(option_history)

        return {'futures': historic_future_prices, 'options': historic_option_prices}

    def _convert_tick_to_percentage_strike(self, strike: float, tick: dict) -> dict:

        return {'volume': tick['volume'],
                'open': strike / tick['open'],
                'high': strike / tick['high'],
                'low': strike / tick['low'],
                'close': strike / tick['close'],
                'cost': tick['cost'],
                }

    def _extract_overlapping_dates(self, historic_futures, historic_options):

        result = {}

        for option, ticks in historic_options.items():

            split = option.split('-')
            future = split[0] + '-' + split[1]
            strike = float(split[2])
            option_type = split[3]
            expiry = split[1]
            future_ticks = historic_futures.get(future, {})

            for tick, prices in ticks.items():
                if tick in future_ticks:

                    if option not in result:
                        result[option] = {}

                    prices['strike'] = strike
                    prices['option_type'] = option_type
                    prices['expiry'] = expiry
                    prices['calculation_date'] = datetime.fromtimestamp(tick/1000).strftime('%y%m%d')
                    date_exp = datetime.strptime(prices['expiry'], '%y%m%d')
                    date_calc = datetime.strptime(prices['calculation_date'], '%y%m%d')
                    delta = date_exp - date_calc
                    prices['term'] = delta.days
                    prices['exchange_date'] = datetime.fromtimestamp(tick / 1000)
                    result[option][tick] = {
                        'option': prices,
                        'future': self._convert_tick_to_percentage_strike(strike, future_ticks[tick])
                    }

                    # print("MATCHED", result[option][tick])

        return result

    def _convert_prices_to_vols(self, historic_vol_dates) -> list:

        result = {}

        for option, ticks in historic_vol_dates.items():
            # print("DO VOL", option, len(ticks))
            for tick, price_data in ticks.items():

                if price_data['option']['term'] > 0:
                    self._get_tick_implied_vols(option, tick, price_data)

                    if option not in result:
                        result[option] = {}

                    result[option][tick] = price_data['option']

                    prices = ['open', 'high', 'low', 'close']
                    for price in prices:
                        result[option][tick][price + "_strike"] = price_data['future'][price] * 100

        result_table = []

        for option, ticks in result.items():
            for tick, vols in ticks.items():
                row = {'symbol': option,
                       'timestamp': tick,
                       }
                row.update(vols)
                result_table.append(row)

        return result_table

    def _get_last_price_update(self, symbol):

        self.db_cursor.execute(f'''SELECT max(ExchangeTimestamp)
                                FROM '{self.deribit_ohlcv}'
                                WHERE MarketSymbol = '{symbol}'
                                AND Exchange = 'deribit';
                                ''')
        last_update_time_ms = self.db_cursor.fetchone()

        if not last_update_time_ms[0]:
            return 0

        return last_update_time_ms[0]

    def _get_last_vol_update(self, symbol):

        self.db_cursor.execute(f'''SELECT max(ExchangeTimestamp)
                                FROM '{self.deribit_ohlcv_vol}'
                                WHERE MarketSymbol = '{symbol}'
                                AND Exchange = 'deribit';
                                ''')
        last_update_time_ms = self.db_cursor.fetchone()

        if not last_update_time_ms[0]:
            return 0

        return last_update_time_ms[0]

    def _push_historic_vols_to_db(self, historic_vols):

        now = datetime.utcnow()
        rowcount = 0
        for i, ohlcv_row in enumerate(historic_vols):
            last_update = self._get_last_vol_update(ohlcv_row['symbol'])
            # print(ohlcv_row['symbol'], last_update)
            if ohlcv_row['timestamp'] > last_update:
                self.db_cursor.execute(f'''
                        INSERT INTO {self.deribit_ohlcv_vol}
                        VALUES(%s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, 
                                %s, %s, %s, %s,
                                %s, %s);
                        ''',
                               (now, 'deribit', ohlcv_row['symbol'], ohlcv_row['exchange_date'], ohlcv_row['timestamp'],
                                ohlcv_row['open_vol'], ohlcv_row['high_vol'], ohlcv_row['low_vol'], ohlcv_row['close_vol'],
                                ohlcv_row['open_strike'], ohlcv_row['high_strike'], ohlcv_row['low_strike'], ohlcv_row['close_strike'],
                                ohlcv_row['term'], ohlcv_row['volume']))
                rowcount += 1

            if (i + 1) % 1000 == 0:
                self.db_connection.commit()
                print("COMMITTING VOLS", rowcount, "after", i+1, "out of", len(historic_vols))

        self.db_connection.commit()
        print("COMMITTED VOLS", rowcount, "out of", len(historic_vols))
        return rowcount

    def _convert_prices_to_data_table(self, historic_prices) -> list:

        # print("FUTURES", historic_prices['futures'].keys())
        result_table = []

        futures = historic_prices['futures']
        options = historic_prices['options']

        for future, ticks in futures.items():
            # print("DO FUTURE", future, len(ticks))
            for tick, price_data in ticks.items():
                row = {'symbol': future,
                       'timestamp': tick,
                       'exchange_date': datetime.fromtimestamp(tick / 1000)
                       }
                row.update(price_data)
                result_table.append(row)

        for option, ticks in options.items():
            # print("DO OPTION", option, len(ticks))
            for tick, price_data in ticks.items():
                row = {'symbol': option,
                       'timestamp': tick,
                       'exchange_date': datetime.fromtimestamp(tick / 1000)
                       }
                row.update(price_data)
                result_table.append(row)

        return result_table


    def _push_historic_prices_to_db(self, historic_prices):

        historic_prices_data_table = self._convert_prices_to_data_table(historic_prices)

        now = datetime.utcnow()
        rowcount = 0
        for i, ohlcv_row in enumerate(historic_prices_data_table):
            last_update = self._get_last_price_update(ohlcv_row['symbol'])
            # print(ohlcv_row['symbol'], ohlcv_row['timestamp'], last_update)
            if ohlcv_row['timestamp'] > last_update:
                self.db_cursor.execute(f'''
                        INSERT INTO {self.deribit_ohlcv}
                        VALUES(%s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, 
                                %s);
                        ''',
                               (now, 'deribit', ohlcv_row['symbol'], ohlcv_row['exchange_date'], ohlcv_row['timestamp'],
                                ohlcv_row['open'], ohlcv_row['high'], ohlcv_row['low'], ohlcv_row['close'],
                                ohlcv_row['volume']))
                rowcount += 1

            if (i + 1) % 1000 == 0:
                self.db_connection.commit()
                print("COMMITTING PRICES", rowcount, "after", i+1, "out of", len(historic_prices_data_table))

        self.db_connection.commit()
        print("COMMITTED PRICES", rowcount, "out of", len(historic_prices_data_table))
        return rowcount


    def _process_historic_ohlcv(self):

        historic_instruments = self._get_option_and_future_instruments()
        # print("HISTORIC INSTRUMENTS", historic_instruments)

        # loop per year/month
        # years = ['17', '18', '19' , '20', '21', '22', '23']
        # months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        years = ['23']
        months = ['05', '06', '07', '08', '09', '10', '11', '12']
        for year in years:
            for month in months:
                period = year + month
                print("PROCESSING PERIOD YYMM", period)
                historic_prices = self._get_option_and_future_prices(historic_instruments, period)
                print("PROCESSING PRICES", "futures count:", len(historic_prices['futures']), "options count:", len(historic_prices['options']))
                # print("HISTORIC PRICES", historic_prices)
                # Push the historic price data to the database
                self._push_historic_prices_to_db(historic_prices)

                # calculate implied vols for the matched options
                historic_vol_dates = self._extract_overlapping_dates(historic_prices['futures'], historic_prices['options'])
                historic_implied_vols = self._convert_prices_to_vols(historic_vol_dates)

                # push the historic implied vols to the database
                self._push_historic_vols_to_db(historic_implied_vols)

        return

    # def process_historic_ohlcv(self):
    #     """ Insert all the historic price data for swaps, futures and options for deribit
    #     """
    #     try:
    #         return self._process_historic_ohlcv()
    #         # process_ohlcv_implied_vol(db_cursor, db_connection, markets)
    #     except Exception as e:
    #         print(f"An exception has occurred: {e}")
    #     finally:
    #         if self.db_cursor:
    #             self.db_cursor.close()
    #         if self.db_connection:
    #             self.db_connection.close()
    #         print('Postgres connection is closed.')

    def check_ohlcv_price_table_exists(self):
        self.db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.deribit_ohlcv} (
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

    def check_ohlcv_vol_table_exists(self):
        self.db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.deribit_ohlcv_vol} (
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


# def historic_data(instrument):
#
#     expiry = instrument.split('-')[1]
#     expiry_timestamp = int((datetime.strptime(expiry, '%d%b%y').timestamp() + 86400)*1000)
#     print(expiry, expiry_timestamp)
#
#     session = requests.Session()
#     history_url = "https://history.deribit.com"
#     # return expiry, int(expiry_timestamp)
#
#     action = "/api/v2/public/get_tradingview_chart_data"
#     params = {'instrument_name': instrument,
#               'include_old': 'true',
#               'start_timestamp': 0,
#               'end_timestamp': expiry_timestamp,
#               'resolution': '1D'
#               }
#     response = session.get(history_url + action, params=params)
#
#     print("RESPONSE", instrument, response.json()['result'])
#
#     return response.json()['result']


if __name__ == "__main__":

    deribit_history = DeribitHistory()

    deribit_history.check_ohlcv_vol_table_exists()
    deribit_history.check_ohlcv_price_table_exists()

    results = deribit_history._process_historic_ohlcv()

    # print("RESULTS", results)

    # for option, ticks in results.items():
    #     for tick, vols in ticks.items():
    #         print(option, tick, vols)
    # history = historic_data('BTC-31MAR17')
    #
    # for i, open_price in enumerate(history['open'][:100]):
    #     date = datetime.fromtimestamp(history['ticks'][i]/1000)
    #     print(i, date, open_price)
    #
    # history = historic_data('BTC-31MAR17-800-C')
    #
    # for i, open_price in enumerate(history['open'][:100]):
    #     date = datetime.fromtimestamp(history['ticks'][i]/1000)
    #     print(i, date, open_price)