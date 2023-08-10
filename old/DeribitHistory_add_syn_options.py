import time
import tomli
import requests
from datetime import datetime
import psycopg2
import QuantLib as ql


class DeribitHistory_add_syn_options:

    def __init__(self):

        self.deribit_ohlcv_vol = "OHLCV_VOL_SYN"
        self.deribit_ohlcv = "OHLCV"

        self.db_config: dict = self._load_config()
        self.db_cursor = None
        self.db_connection = None
        self._connectDB(self.db_config)

        self._check_table_exists()

        self._futures_historic_curves = {}

    def _check_table_exists(self):

        self.db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.deribit_ohlcv_vol} (
                                    ts TIMESTAMP,
                                    Exchange  STRING NOT NULL,
                                    MarketSymbol  STRING NOT NULL,
                                    ExchangeDay TIMESTAMP NOT NULL,
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
                                    OpenDelta  FLOAT,
                                    CloseDelta FLOAT,
                                    Term FLOAT,
                                    Volume  FLOAT
                            ) timestamp(ts);''')

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

        with open("../DeribitPriceHistoryDBGateway.toml", mode="rb") as cf:
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

        print("CALC IMPLIED VOL FOR DATE", calculation_date, 'EXPIRY', expiry_date, 'STRIKE', strike, 'FUTURE', underlying, 'MARK', mark_price, 'type', call_put)

        option_type = ql.Option.Call
        if call_put == "P":
            option_type = ql.Option.Put

        def _date_split(calculation_date: str):
            """ Utility for splitting the date string into parts """
            print("DATE", calculation_date)
            year = int("20" + calculation_date[:2])
            month = int(calculation_date[2:4])
            day = int(calculation_date[-2:])
            return day, month, year

        print("DATE SPLIT", *_date_split(calculation_date))
        today = ql.Date(*_date_split(calculation_date))
        print("TOSAY", today)

        ql.Settings.instance().evaluationDate = today

        expiry = ql.Date(*_date_split(expiry_date))

        # The Instrument
        option = ql.EuropeanOption(ql.PlainVanillaPayoff(option_type, strike),
                                   ql.EuropeanExercise(expiry))
        # The Market
        u = ql.SimpleQuote(underlying)  # set todays value of the underlying
        r = ql.SimpleQuote(risk_free_rate / 100)  # set risk-free rate
        sigma = ql.SimpleQuote(0.5)  # set volatility
        riskFreeCurve = ql.FlatForward(0, ql.NullCalendar(), ql.QuoteHandle(r), ql.Actual360())
        volatilityCurve = ql.BlackConstantVol(0, ql.NullCalendar(), ql.QuoteHandle(sigma), ql.Actual365Fixed())
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
                # print("OPTION HISTORY", option['symbol'])
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

    def _calculate_term(self, exchange_date, expiry):
        """ Days from exchange date to expiry date
        """

        if type(exchange_date) is str:
            date_calc = datetime.strptime(exchange_date, '%y%m%d')
        else:
            date_calc = exchange_date

        if type(expiry) is str:
            date_exp = datetime.strptime(expiry, '%y%m%d')
        else:
            date_exp = expiry

        delta = date_exp - date_calc
        return delta.days

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
                    prices['term'] = self._calculate_term(prices['calculation_date'], prices['expiry'])
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

        rowcount = 0
        for i, ohlcv_row in enumerate(historic_vols):
            last_update = self._get_last_vol_update(ohlcv_row['symbol'])
            # print(ohlcv_row['symbol'], last_update)
            if ohlcv_row['timestamp'] > last_update:
                now = datetime.utcnow()
                exchange_date = datetime.fromtimestamp(ohlcv_row['timestamp'] / 1000)
                exchange_day = exchange_date.replace(hour=0, minute=0, second=0, microsecond=0)
                self.db_cursor.execute(f'''
                        INSERT INTO {self.deribit_ohlcv_vol}
                        VALUES(%s, %s, %s, 
                                %s, %s, %s,
                                %s, %s, %s, %s, 
                                %s, %s, %s, %s,
                                %s, %s);
                        ''',
                               (now, 'deribit', ohlcv_row['symbol'],
                                exchange_day, ohlcv_row['exchange_date'], ohlcv_row['timestamp'],
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

        rowcount = 0
        for i, ohlcv_row in enumerate(historic_prices_data_table):
            last_update = self._get_last_price_update(ohlcv_row['symbol'])
            # print(ohlcv_row['symbol'], ohlcv_row['timestamp'], last_update)
            if ohlcv_row['timestamp'] > last_update:
                now = datetime.utcnow()
                exchange_date = datetime.fromtimestamp(ohlcv_row['timestamp'] / 1000)
                exchange_day = exchange_date.replace(hour=0, minute=0, second=0, microsecond=0)
                self.db_cursor.execute(f'''
                        INSERT INTO {self.deribit_ohlcv}
                        VALUES(%s, %s, %s, 
                                %s, %s, %s,
                                %s, %s, %s, %s, 
                                %s);
                        ''',
                               (now, 'deribit', ohlcv_row['symbol'],
                                exchange_day, ohlcv_row['exchange_date'], ohlcv_row['timestamp'],
                                ohlcv_row['open'], ohlcv_row['high'], ohlcv_row['low'], ohlcv_row['close'],
                                ohlcv_row['volume']))
                rowcount += 1

            if (i + 1) % 1000 == 0:
                self.db_connection.commit()
                print("COMMITTING PRICES", rowcount, "after", i+1, "out of", len(historic_prices_data_table))

        self.db_connection.commit()
        print("COMMITTED PRICES", rowcount, "out of", len(historic_prices_data_table))
        return rowcount

    def _get_historic_vol_data(self):
        """ Load all historic vol data and return a 'set' for check if vol exists
         """

        # columns = ['ts', 'exchange', 'symbol', 'exchange_day', 'exchange_date', 'exchange_ts',
        #            'open_vol', 'high_vol', 'low_vol', 'close_vol',
        #            'open_strike', 'high_strike', 'low_strike', 'close_strike',
        #            'term', 'volume']

        self.query_string = f"SELECT * from {self.deribit_ohlcv_vol}"

        # print(self.query_string)

        self.db_cursor.execute(self.query_string)
        result = self.db_cursor.fetchall()

        return {(option[1], option[2], option[3]) for option in result}

    def _convert_prices_to_curves(self, future_prices):
        """ **Currently only do options for BTC / ETH for deribit
            Convert the futures prices into a 'curve' as a collection
            of future prices for each exchange date and future expiry term.
            perpetuals have expiry 0
        """

        curves = {}

        for future in future_prices:

            # skip if not deribit
            if future[1] != 'deribit':
                continue

            split = future[2].split('-')
            token = split[0]

            # skip non BTC/ETH future price data
            if token[:3] not in ['BTC', 'ETH']:
                continue

            try:
                expiry = split[1]
            except IndexError:
                expiry = future[3]

            key = (future[1], token, future[3])

            if key not in curves:
                curves[key] = {}

            term = self._calculate_term(future[3], expiry)

            if term not in curves[key]:
                curves[key][term] = future

        return curves




    def _min_float(self, x):

        precision = 0.001

        if x < precision and x > -precision:
            return 0

        if x < -1 + precision:
            return -1

        if x > 1 - precision:
            return 1

        return x

    def _calc_delta(self, strike_pct, term, volatility, call_put):

        option_type = ql.Option.Call
        if call_put == "P":
            option_type = ql.Option.Put

        # print("DATE", calculation_date)
        today = ql.Date.todaysDate()
        # print("TODAY", today)
        ql.Settings.instance().evaluationDate = today

        expiry = today + ql.Period(f'{term}d')
        # print("EXPR", expiry)
        # The Instrument

        option = ql.EuropeanOption(ql.PlainVanillaPayoff(option_type, strike_pct),
                                   ql.EuropeanExercise(expiry))
        # The Market
        u = ql.SimpleQuote(100)  # future is 100
        r = ql.SimpleQuote(0)  # set risk-free rate
        sigma = ql.SimpleQuote(volatility / 100)  # set volatility
        riskFreeCurve = ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(r), ql.Actual360())
        volatilityCurve = ql.BlackConstantVol(0, ql.TARGET(), ql.QuoteHandle(sigma), ql.Actual365Fixed())
        # The Model
        process = ql.BlackProcess(ql.QuoteHandle(u),
                                  ql.YieldTermStructureHandle(riskFreeCurve),
                                  ql.BlackVolTermStructureHandle(volatilityCurve))
        # The Pricing Engine
        engine = ql.AnalyticEuropeanEngine(process)
        # The Result
        option.setPricingEngine(engine)
        # print("OPTION PRICER", option.NPV(), option.delta(), option.gamma(), option.impliedVolatility(option.NPV(), process))
        results = {"mark_price": option.NPV(),
                   'term': term,
                   'strike_pct': strike_pct,
                   "delta": self._min_float(option.delta()),
                   'gamma': self._min_float(option.gamma()),
                   'vega': self._min_float(option.vega() / 100),
                   'theta': self._min_float(option.theta() / 365),
                   'rho': -self._min_float((expiry - today) / 360 * option.NPV() / 100)
                   }

        return results

    def _get_delta(self, record, type):

        vol = record[6]
        strike_pct = record[10]

        if type == 'close':
            vol = record[9]
            strike_pct = record[13]

        term = record[14]
        call_put = record[2][-1]

        return self._calc_delta(strike_pct, term, vol, call_put)['delta']




    def _process_delta(self, vol):

        now = datetime.utcnow()

        delta_open = self._get_delta(vol, 'open')
        delta_close = self._get_delta(vol, 'close')

        self.db_cursor.execute(f'''
                                INSERT INTO {self.deribit_ohlcv_vol_delta}
                                VALUES(%s, %s, %s, 
                                        %s, %s, %s,
                                        %s, %s, %s, %s, 
                                        %s, %s, %s, %s,
                                        %s, %s,
                                        %s, %s);
                                ''',
                               (now, vol[1], vol[2],
                                vol[3], vol[4], vol[5],
                                vol[6], vol[7], vol[8], vol[9],
                                vol[10], vol[11], vol[12], vol[13],
                                delta_open, delta_close,
                                vol[14], vol[15]))


    def process_missing_syn_options(self):
        """ Get a list of all the prices we have to find options that we have no vol for.
            when we find one, calculate the vol entry based on linear interpolation of the associated future price
        """

        historic_future_curves, historic_option_prices = self._get_historic_price_data()
        historic_vols = self._get_historic_vol_data()

        print("FUTURE CURVES", len(historic_future_curves))
        print("OPTION PRICES", len(historic_option_prices))
        print("VOLS", len(historic_vols))

        self._process_historic_data(historic_future_curves, historic_option_prices, historic_vols)

    def _vol_is_missing(self, option, historic_vols):

        key = (option[1], option[2], option[3])

        return not (key in historic_vols)

    def _get_historic_future_price(self, option_price, historic_future_prices):
        """ Using option expiry, interpolate on the futures curve for an underlying price
        """

        # key = option_price

    def _interpolate_future_prices(self, key, term, historic_futures):
        """ Interpolate the future open/close/high/low prices
            for the given key and term
        """

        # print()
        # print("INTERPOLATE", key, "TERM", term)
        # print("INTERPOLATE FROM", sorted(historic_futures[key].keys()))

        future_terms = list(sorted(historic_futures[key].keys()))
        result = None

        if term in future_terms:
            print("***** THIS IS ODD; TERM", term, "ALREADY EXISTS!", future_terms)
            result = [historic_futures[key][term][6], historic_futures[key][term][7], historic_futures[key][term][8], historic_futures[key][term][9]]
            return result

        before_term = None
        after_term = None

        for future_term in future_terms:
            if future_term < term:
                before_term = future_term
            if future_term > term:
                after_term = future_term
                break

        if before_term is None:
            # return data of first future price
            print("***** BEFORE; THIS IS VERY ODD!!!")
            term = 0
            result = [historic_futures[key][term][6], historic_futures[key][term][7], historic_futures[key][term][8], historic_futures[key][term][9]]
            return result

        if after_term is None:
            print("***** AFTER")
            # return data of final future price
            term = future_terms[-1]
            result = [historic_futures[key][term][6], historic_futures[key][term][7], historic_futures[key][term][8], historic_futures[key][term][9]]
            return result

        # print("LOOKING FOR", term, "BETWEEN", before_term, after_term)
        factor = (term - before_term)/(after_term - before_term)
        future = historic_futures[key]
        result = [future[before_term][6]*(1-factor) + future[after_term][6]*factor,
                  future[before_term][7]*(1-factor) + future[after_term][7]*factor,
                  future[before_term][8]*(1-factor) + future[after_term][8]*factor,
                  future[before_term][9]*(1-factor) + future[after_term][9]*factor,
        ]

        return result

    def _update_syn_vols_in_db(self, future_prices, option_price):
        """ Do OHLC Vol Interpolation and write to database
        """

        calculation_date = option_price[3].strftime('%y%m%d')

        split = option_price[2].split('-')
        strike = float(split[2])
        expiry = split[1]
        call_put = split[3]

        prices = ['open', 'high', 'low', 'close']
        future = {'open': future_prices[0], 'high': future_prices[1], 'low': future_prices[2], 'close': future_prices[3]}
        option = {'open': option_price[6], 'high': option_price[7], 'low': option_price[8], 'close': option_price[9]}

        for price in prices:
            underlying = future[price]
            mark_price = option[price]
            vol_price = self._calc_implied_vol(strike, underlying, calculation_date, expiry, mark_price, call_put)
            option[price + "_vol"] = vol_price

        print("GOT IMPLIED VOLS", option_price[2], option)

    def _process_syn_option(self, option_price, historic_future_prices):
        """ Calculate the option implied vol from interpolating on the historic future prices
        """

        split = option_price[2].split('-')
        future_key = (option_price[1], split[0], option_price[3])
        expiry = split[1]
        calc_date = option_price[3]

        term = self._calculate_term(calc_date, expiry)

        if term > 0:
            # print("PROCESS MISSING OPTION", option_price[2], "ON", option_price[3], "PRICE", option_price[6])
            future_prices = self._interpolate_future_prices(future_key, term, historic_future_prices)
            # print("GOT INTERPOLATED PRICES", future_prices)

            if future_prices:
                self._update_syn_vols_in_db(future_prices, option_price)

        return 0

    def _process_historic_data(self, historic_future_curves, historic_option_prices, historic_vols):
        """ Skip through all the option prices we have, and if the Vol is missing,
            calculate the implied vol and add it.
        """

        missing = 0

        for i, option_price in enumerate(historic_option_prices[1000000:10010000:100]):
            if self._vol_is_missing(option_price, historic_vols):
                # print(i, option_price[2])
                missing += 1
                self._process_syn_option(option_price, historic_future_curves)

            if i > 10000:
                break

            # if (i + 1) % 1000 == 0:
            #     self.db_connection.commit()
            #     print("COMMITTING VOL DELTAS", i+1,  "out of", len(historic_vols))

        # self.db_connection.commit()

        print("MISSING VOLS COUNT", missing)

        return




if __name__ == "__main__":

    deribit_history = DeribitHistory_add_syn_options()

    deribit_history.process_missing_syn_options()

    # ql.Settings.instance().evaluationDate = ql.Date(12,11,2022)
    #
    # print("EVAL", ql.Settings.instance().evaluationDate)