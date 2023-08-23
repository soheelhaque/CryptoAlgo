import tomli
from datetime import datetime
import psycopg2
import QuantLib as ql
import logging, time, sys, getopt
import logging.handlers as handlers


logger = logging.getLogger('DERIBIT VOL UPDATER')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logHandler = handlers.RotatingFileHandler('deribit_vol_updater.log', maxBytes=50000, backupCount=5)
logHandler.setLevel(logging.INFO)

logHandler.setFormatter(formatter)

logger.addHandler(logHandler)


class DeribitVolHistoryDBUpdate:
    """ This module will populate all rows missing from the historic vol table.
        It will create the vol history table if it does not exist.

        Rows are generally missing because of gaps in running the daily price update.
        Or, obviously, when initialising a new database or historic vol database table

        Missing vol rows are calculated using data from the OHLCV price history table.
        So, to add missing Vol history, you first need to add any missing Price history (see DeribitPriceHistoryDBGateway.py)

        The vol data consists of the open/close implied volatility, strike% and delta for Deribit options.

    """

    def __init__(self):

        self.deribit_ohlcv = "OHLCV"
        self.deribit_ohlcv_vol = "OHLCV_VOL"

        self.db_config: dict = self._load_config()
        self.db_cursor = None
        self.db_connection = None
        self._connectDB(self.db_config)

        self._check_vol_history_table_exists()

    def _ensure_datetime(self, given_date) -> datetime:
        """ Ensures given date is a python datetime object.
            Converts type string to datetime if required.
        """
        if type(given_date) is str:
            date_calc = datetime.strptime(given_date, '%y%m%d')
        else:
            date_calc = given_date

        return date_calc

    def _calculate_term(self, exchange_date, expiry) -> int:
        """ Days from exchange date to given expiry date
        """

        date_calc = self._ensure_datetime(exchange_date)
        date_exp = self._ensure_datetime(expiry)

        delta = date_exp - date_calc
        return delta.days

    def _check_vol_history_table_exists(self) -> None:
        """ IF Vol History table does not exist, then create it
        """

        self.db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.deribit_ohlcv_vol} (
                                    ts TIMESTAMP,
                                    Exchange  STRING NOT NULL,
                                    MarketSymbol  STRING NOT NULL,
                                    ExchangeDay TIMESTAMP NOT NULL,
                                    ExchangeDate TIMESTAMP NOT NULL,
                                    ExchangeTimestamp LONG NOT NULL,
                                    OpenVol  FLOAT,
                                    OpenStrike  FLOAT,
                                    OpenDelta  FLOAT,
                                    CloseVol FLOAT,
                                    CloseStrike FLOAT,
                                    CloseDelta FLOAT, 
                                    Term FLOAT,
                                    Volume  FLOAT
                            ) timestamp(ts);''')

    def _connectDB(self, db_config) -> None:
        """ Establish connection to the database using given config
        """

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
        """ Load up the database configuration .toml file
        """

        db_config = {}

        # Use same .toml file as the Deribt Price History...
        with open("DeribitPriceHistoryDBGateway.toml", mode="rb") as cf:
            config = tomli.load(cf)

            # database config
            db_config['user'] = config['database']['user']
            db_config['password'] = config['database']['password']
            db_config['host'] = config['database']['host']
            db_config['port'] = config['database']['port']
            db_config['database'] = config['database']['database']

        return db_config

    def _delta_as_float(self, x) -> float:
        """ Ensure delta figure is presented as a float for DB insert
        """

        precision = 0.001

        if x < precision and x > -precision:
            return 0

        if x < -1 + precision:
            return -1

        if x > 1 - precision:
            return 1

        return x

    def _convert_prices_to_curves(self, future_prices) -> dict:
        """ Converts all the available perpetual and futures prices into curves.
            Each curve is for a given token and exchange date and consists
            of a set of future prices indexed by 'term' ie time to expiry.

            Filters are also applied to restrict the curves to only those useful
            as underlyings for options.

        """

        # print("CONVERTING FUTURE PRICES TO CURVES...")

        curves = {}

        for future in future_prices:

            # skip if not deribit
            if future[1] != 'deribit':
                continue

            split = future[2].split('-')

            # skip non BTC/ETH future price data
            if future[2][:3] not in ['BTC', 'ETH']:
                continue

            try:
                expiry = split[1]
            except IndexError:
                expiry = future[3]

            key = self._future_key_from_record(future)

            if key not in curves:
                curves[key] = {}

            term = self._calculate_term(future[3], expiry)

            if term not in curves[key]:
                curves[key][term] = future

        return curves

    def _get_historic_price_data(self, year: int, month: int) -> (dict, list):
        """ Load all historic price data required for Vol interpolation.

            :param year: the year to process
            :param month: the month to process

            Return dictionary of future curves and list of available option prices
        """

        # columns = ['ts', 'exchange', 'symbol', 'exchange_day', 'exchange_date', 'exchange_ts',
        #            'open', 'high', 'low', 'close',
        #            'volume']

        # print(f"LOADING PRICE DATA...{year}-{month}")

        where_clause = self._where_clause(year, month)
        # print("WHERE CLAUSE", where_clause)
        self.query_string = f"""SELECT * from {self.deribit_ohlcv} 
                                where {where_clause}"""

        # print(self.query_string)

        self.db_cursor.execute(self.query_string)
        result = self.db_cursor.fetchall()

        future_prices = []
        option_prices = []

        for price in result:
            # pick up perpetuals and futures
            if (':' in price[2]) and (len(price[2].split('-')) <= 2):
                future_prices.append(price)
                continue

            # pick up just options
            if len(price[2].split('-')) == 4:
                option_prices.append(price)

        # Collect future prices into a dictionary of 'curves' for each COB date
        future_curves = self._convert_prices_to_curves(future_prices)

        return future_curves, option_prices

    def _get_existing_historic_vol_keys(self, year, month) -> set:
        """ Load all historic vol data and return a 'set' of 'keys'
            to enable fast check if vol record already exists in the database
            key is exchange + symbol + COB Date

            :param year: the year to process
            :param month: the month to process

         """

        # print(f"LOADING HISTORIC VOL DATA...{year}-{month}")

        # columns = ['ts', 'exchange', 'symbol', 'exchange_day', 'exchange_date', 'exchange_ts',
        #            'open_vol', 'high_vol', 'low_vol', 'close_vol',
        #            'open_strike', 'high_strike', 'low_strike', 'close_strike',
        #            'term', 'volume']

        where_clause = self._where_clause(year, month)

        self.query_string = f"""SELECT * from {self.deribit_ohlcv_vol} 
                                where {where_clause}"""

        # print(self.query_string)

        self.db_cursor.execute(self.query_string)
        result = self.db_cursor.fetchall()

        # key is exchange + symbol + COB Date
        return {self._option_key_from_record(option) for option in result}

    def _option_key_from_record(self, record) -> tuple:
        """ return a standard unique 'key' from the given record data
        """

        # key is exchange + symbol + COB Date
        return (record[1], record[2], str(record[3].strftime('%Y-%m-%d')))

    def _future_key_from_record(self, record) -> tuple:
        """ Calculate a unique future key as just the perpetual name or future name
            or, if an option, the underlying future name
        """
        split = record[2].split('-')
        token = split[0]

        # key is exchange + symbol (without strike/option_type, if present) + COB Date
        return (record[1], token, record[3].strftime('%Y-%m-%d'))

    def _where_clause(self, year, month):
        """ construct a date where clouse to restrict results to a single month
        """
        start_date = f"{year}-{month:02}-01"
        end_month = month + 1 if month < 12 else 1
        end_year = year if month < 12 else year + 1
        end_date = f"{end_year}-{end_month:02}-01"

        return f"ExchangeDay >= '{start_date}' AND ExchangeDay < '{end_date}'"

    def _vol_record_exists(self, option, historic_vol_keys) -> bool:
        """ Check if an option has already got a record in the historic vols table
        """
        return self._option_key_from_record(option) in historic_vol_keys

    def _get_missing_historic_vols(self, year, month) -> (dict, list):
        """ Determine list of option prices that have no corresponding historic Vol data
            and have a chance of being able to calculate a valid historic volatility.

            :param year: the year to process
            :param month: the month to process

            Return list of options to consider, along with the associated historic future curves
        """

        future_curves, option_prices = self._get_historic_price_data(year, month)

        if not future_curves:
            return future_curves, option_prices

        existing_historic_vol_keys = self._get_existing_historic_vol_keys(year, month)

        # print("EXISTING", list(existing_historic_vol_keys)[:50])

        missing_option_vols = []
        key_count, term_count, exists_count, missing_count = 0, 0, 0, 0

        # option_prices = option_prices[1273000:]

        # restrict set of options to only those that are missing and have a futures curve
        for option_price in option_prices:

            # print(self._option_key_from_record(option_price))

            if self._vol_record_exists(option_price, existing_historic_vol_keys):
                # print("record exists", option_price)
                exists_count += 1
                continue
            else:
                missing_count += 1

            # print("MISSING", self._option_key_from_record(option_price))
            split = option_price[2].split('-')
            future_key = self._future_key_from_record(option_price)
            expiry = split[1]
            calc_date = option_price[3]

            term = self._calculate_term(calc_date, expiry)

            if term <= 0:
                # print("term zero", term)
                term_count += 1
                continue

            if future_key not in future_curves:
                key_count +=1
                # print("future missing", future_key)
                continue

            # print("existing", calc_date, option_price[2], future_key)
            missing_option_vols.append(option_price)

        # print("VOL ANALYSIS: OUT OF", len(option_prices), "PRICES", exists_count, "VOLS ALREADY EXIST,", missing_count,"VOLS ARE MISSING")
        self.info_logger(f"WILL PROCESS: {len(missing_option_vols)} SKIPPING: {term_count} HAVE TERM ZERO, AND {key_count} HAVE NO FUTURES PRICES" )

        return future_curves, missing_option_vols

    def _underlying_price(self, oh_flag, future_curve, term) -> float:
        """ Interpolate the future open/close prices
            for the given future curve and term
        """

        future_terms = list(sorted(future_curve.keys()))

        if oh_flag == 'open':
            oh_price = 6
        else:
            oh_price = 9

        if term in future_terms:
            return future_curve[term][oh_price]

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
            # print(f"***** BEFORE; THIS IS VERY ODD!!! OPTION TERM {term} COMES BEFORE FIRST FUTURE {future_terms[0]}")
            # print("INPUTS", oh_flag, future_curve, term)
            term = future_terms[0]
            return future_curve[term][oh_price]

        if after_term is None:
            # print(f"***** AFTER FINAL FUTURE - EXTRAPOLATING {term}")
            # return data of final future price
            term = future_terms[-1]
            return future_curve[term][oh_price]

        # print("LOOKING FOR", term, "BETWEEN", before_term, after_term)
        factor = (term - before_term) / (after_term - before_term)

        return future_curve[before_term][oh_price] * (1 - factor) + future_curve[after_term][oh_price] * factor

    def _calc_implied_vol_strike_and_delta(self, oh_flag, option_price, future_curve, calculation_date, expiry_date, term, risk_free_rate=0.0) -> list:
        """ Given the optionand future price data, calculate the associated
            implied vol, strike_pct and delta using either the open or close prices.
        """
        strike = float(option_price[2].split('-')[2])
        underlying_price = self._underlying_price(oh_flag, future_curve, term)

        if oh_flag == 'open':
            mark_price = option_price[6]
        else:
            mark_price = option_price[9]
        call_put = option_price[2].split('-')[3]


        # print("CALC IMPLIED VOL FOR DATE", calculation_date, 'EXPIRY', expiry_date, 'STRIKE', strike, 'FUTURE',
        #       underlying_price, 'MARK', mark_price, 'type', call_put)

        # Just in case!
        if underlying_price <= 0:
            return None

        option_type = ql.Option.Call
        if call_put == "P":
            option_type = ql.Option.Put

        def _date_split(given_date: datetime) -> (int, int, int):
            """ Utility for converting datetime t """
            return given_date.day, given_date.month, given_date.year

        today = ql.Date(*_date_split(calculation_date))
        expiry = ql.Date(*_date_split(expiry_date))

        # set calc date
        ql.Settings.instance().evaluationDate = today
        # The Instrument
        option = ql.EuropeanOption(ql.PlainVanillaPayoff(option_type, strike),
                                   ql.EuropeanExercise(expiry))
        # Calculate Implied Vol
        # The Market
        u = ql.SimpleQuote(underlying_price)  # set todays value of the underlying
        r = ql.SimpleQuote(risk_free_rate / 100)  # set risk-free rate
        sigma = ql.SimpleQuote(0.5)  # set volatility
        riskFreeCurve = ql.FlatForward(0, ql.NullCalendar(), ql.QuoteHandle(r), ql.Actual360())
        volatilityCurve = ql.BlackConstantVol(0, ql.NullCalendar(), ql.QuoteHandle(sigma), ql.Actual365Fixed())
        # The Model
        process = ql.BlackProcess(ql.QuoteHandle(u),
                                  ql.YieldTermStructureHandle(riskFreeCurve),
                                  ql.BlackVolTermStructureHandle(volatilityCurve))

        try:
            # Get USD (or, in general, quote ccy of underlying future) price for quantlib to use
            mark_price_usd = mark_price * underlying_price
            volatility = option.impliedVolatility(mark_price_usd, process) * 100
        except RuntimeError as e:

            if 'root not' in str(e):
                # Vol exceeds bounds <0.0001, > 400.00
                return None

            print(f"ERROR CALC IMPLIED VOL: DATE {calculation_date} for {option_price[2]} : {e}")
            return None

        # Now calculate Delta
        # The Market
        sigma = ql.SimpleQuote(volatility / 100)  # set volatility
        volatilityCurve = ql.BlackConstantVol(0, ql.NullCalendar(), ql.QuoteHandle(sigma), ql.Actual365Fixed())
        # The Model
        process = ql.BlackProcess(ql.QuoteHandle(u),
                                  ql.YieldTermStructureHandle(riskFreeCurve),
                                  ql.BlackVolTermStructureHandle(volatilityCurve))

        engine = ql.AnalyticEuropeanEngine(process)
        # The Result
        option.setPricingEngine(engine)
        # print("OPTION PRICER", option.NPV(), option.delta(), option.gamma(), option.impliedVolatility(option.NPV(), process))
        try:
            delta = self._delta_as_float(option.delta())
        except RuntimeError as e:
            print(f"ERROR CALC DELTA: DATE {calculation_date} for {option_price[2]}: {e}")
            return None

        # Given we have just priced the option, we could compare the usd price we get with
        # the token price passed in to see if they are within some sort of tolerance.

        strike_pct = 100 * strike / underlying_price

        return [volatility, strike_pct, delta]

    def _get_future_curve(self, option_price, future_curves) -> dict:
        """ given the option record, return the future curve required for it to be priced
        """

        future_key = self._future_key_from_record(option_price)
        # print("TRY KEY", future_key)
        return future_curves[future_key]

    def _calculate_missing_vol_data(self, future_curves: dict, option_price: list) -> list:
        """ Calculates and returns a list of vol data that needs to be added to the
            historic vol database. Any error and it will return None
        """

        vol_data = []

        oh_flags = ['open', 'close']

        calculation_date = option_price[3]
        expiry_date = self._ensure_datetime(option_price[2].split('-')[1])
        term = self._calculate_term(calculation_date, expiry_date)
        # print("FUTRE FOR OPTION", option_price[2], option_price[3])
        try:
            future_curve = self._get_future_curve(option_price, future_curves)
        except KeyError:
            print(f"ERROR FUTURE CURVE NOT FOUND {option_price[2]}")
            return None

        for oh_flag in oh_flags:
            vol_strike_delta = self._calc_implied_vol_strike_and_delta(oh_flag, option_price, future_curve, calculation_date, expiry_date, term)

            # print("GOT VOL", option_price[2], vol_strike_delta)

            if not vol_strike_delta:
                return None

            vol_data += vol_strike_delta

        vol_data.append(term)

        # print("OPTION VOL", vol_data)
        return vol_data

    def _insert_missing_vol_row(self, option_vol_row: list) -> None:
        """ Insert the given row into the historic vol database table
        """

        now = datetime.utcnow()

        self.db_cursor.execute(f'''
                                INSERT INTO {self.deribit_ohlcv_vol}
                                VALUES(%s, 
                                        %s, %s, 
                                        %s, %s, %s,
                                        %s, %s, %s, 
                                        %s, %s, %s,
                                        %s, %s);
                                ''',
                               (now,
                                option_vol_row[1], option_vol_row[2],
                                option_vol_row[3], option_vol_row[4], option_vol_row[5],
                                option_vol_row[6], option_vol_row[7], option_vol_row[8],
                                option_vol_row[9], option_vol_row[10], option_vol_row[11],
                                option_vol_row[12], option_vol_row[13])
                               )

    def _process_year_month(self, year: int, month: int) -> None:
        """ Process the price data for the given year and month to find implied vols for options
        """



        future_curves, missing_option_vols = self._get_missing_historic_vols(year, month)

        if not future_curves:
            return

        self.info_logger(f"PROCESSING YEAR {year} MONTH {month}")
        # print("FUTURE KEYS", list(future_curves.keys())[:50])

        failed = 0
        succeded = 0

        for i, option_price in enumerate(missing_option_vols):

            missing_vol_data = self._calculate_missing_vol_data(future_curves, option_price)

            if missing_vol_data:
                # Only add rows where a Vol/delta etc was successfully calculated

                # Capture first few columns shared between prices and vols
                # ts, exchange, symbol, close day, close datetime, timestamp
                option_vol = list(option_price[:6])
                # print("OPTION VOL", option_vol)
                # Add on vol results [open/close: vol, strike_pct, delta]
                option_vol += missing_vol_data
                # Add residual elements of record [volume]
                option_vol.append(option_price[10])

                self._insert_missing_vol_row(option_vol)
                succeded += 1
            else:
                failed += 1

            if (i + 1) % 1000 == 0:
                # Commit updates as we go along...
                self.db_connection.commit()
                # print("PROCESSED VOLS", i + 1, "out of", len(missing_option_vols), "with", succeded, "writes and",
                #       failed, "skipped (term=0 or vol>400)")

        # Finish off any residual commits
        self.db_connection.commit()
        # print("PROCESSED VOLS", len(missing_option_vols), "out of", len(missing_option_vols))
        self.info_logger(f"TOTAL OF {succeded} WRITES AND {failed} SKIPPED (probably already existing, term=0 or vol>400)")

    def info_logger(self, message):

        logger.info(message)
        print("LOG", message)

    def _update_historic_vol_data(self, run_year: int=None, run_month: int=None) -> None:
        """ Iterate through all the option & future price data that we have,
            inserting any data missing from the Vol History table

            :param recent: defaults to true, to only calculate figures for year 2023
        """

        years = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
        months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

        if run_year and not run_month:
            years = [run_year]
            months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

        if run_year and run_month:
            years = [run_year]
            months = [run_month]

        self.info_logger(f"STARTING DeribitVolUpdate: years: {years} months: {months}")

        for year in years:
            for month in months:
                self._process_year_month(year, month)


def get_args(argv):

    opts, args = getopt.getopt(argv,"-hy:m:", ["year=", "month="])

    year = None
    month = None

    for opt, arg in opts:
        if opt == '-h':
            print ('python3 -m DeribitPriceHistoryDBGateway -h -y <2023> -m <6>')
            sys.exit()

        if opt in ("-y", "--year"):
            try:
                year = int(arg)
            except Exception as e:
                print(f'error {e}; year must be format <2023>')
                sys.exit()

        if opt in ("-m", "--month"):
            try:
                month = int(arg)
            except Exception as e:
                print(f'error {e}; month must be format <8>')
                sys.exit()

    if month and not year:
        print(f'error; month can only be provided if year is also provided')
        sys.exit()

    return year, month


if __name__ == "__main__":

    year, month = get_args(sys.argv[1:])

    # print("STARTING HISTORIC VOL UPDATES")

    deribit_history = DeribitVolHistoryDBUpdate()

    deribit_history._update_historic_vol_data(year, month)

    # print("FINISHED HISTORIC VOL UPDATES")

