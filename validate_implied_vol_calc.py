import QuantLib as ql
import psycopg2
from datetime import datetime

def _price_option(strike, underlying, calculation_date: datetime, expiry_date, volatility, call_put, risk_free_rate=0.0):

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
    sigma = ql.SimpleQuote(volatility / 100)  # set volatility
    riskFreeCurve = ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(r), ql.Actual360())
    volatilityCurve = ql.BlackConstantVol(0, ql.TARGET(), ql.QuoteHandle(sigma), ql.Actual365Fixed())
    # The Model
    process = ql.BlackProcess(ql.QuoteHandle(u),
                              ql.YieldTermStructureHandle(riskFreeCurve),
                              ql.BlackVolTermStructureHandle(volatilityCurve))

    engine = ql.AnalyticEuropeanEngine(process)
    # The Result
    option.setPricingEngine(engine)

    try:
        print("CALC PRICE", today, expiry, strike, underlying, volatility, call_put)

        return option.NPV() / underlying
    except RuntimeError as e:

        if 'root not' in str(e):
            return 400
        if 'expired' in str(e):
            return 0
        print("ERROR", e)
        return None


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
        print("CALC VOL", today, expiry, strike, underlying, mark_price * underlying, call_put)

        return option.impliedVolatility(mark_price * underlying, process) * 100
    except RuntimeError as e:

        if 'root not' in str(e):
            return 400
        if 'expired' in str(e):
            return 0
        print("ERROR", e)
        return None

def validate_implied_vol(exchange: str, symbol: str, exchange_date: str) -> None:

    db_user = 'admin'
    db_password = 'quest'
    db_host = '127.0.0.1'
    db_port = 8812
    db_name = 'qdb'
    db_connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port,
                                     database=db_name)
    db_cursor = db_connection.cursor()

    table = 'OHLCV_VOL'
    columns = """Exchange, MarketSymbol AS symbol, ExchangeDate as exchange_date, ExchangeTimestamp as ts, 
                OpenVol, HighVol, LowVol, CloseVol, 
                OpenStrike, HighStrike, LowStrike, CloseStrike, 
                Term, Volume"""
    symbol_clause = f"MarketSymbol = '{symbol}'"
    date_clause = f" AND ExchangeDate = '{exchange_date}'"
    exchange_clause = f" AND Exchange = '{exchange}'"

    query_string = f"SELECT {columns} from {table} where {symbol_clause} {date_clause} {exchange_clause}"
    db_cursor.execute(query_string)
    option_vol = db_cursor.fetchall()[0]

    table = 'OHLCV'
    columns = """Exchange, MarketSymbol AS symbol, ExchangeDate as exchange_date, ExchangeTimestamp as ts, 
                Open, High, Low, Close, 
                Volume"""

    query_string = f"SELECT {columns} from {table} where {symbol_clause} {date_clause} {exchange_clause}"
    # print("OPTION PRICE QUERY", query_string)
    db_cursor.execute(query_string)
    option_price = db_cursor.fetchall()[0]

    future_name = symbol.split('-')[0] + '-' + symbol.split('-')[1]
    symbol_clause = f"MarketSymbol = '{future_name}'"
    query_string = f"SELECT {columns} from {table} where {symbol_clause} {date_clause} {exchange_clause}"
    db_cursor.execute(query_string)
    future_price = db_cursor.fetchall()[0]

    print("Option Vol", option_vol)
    print("Option price", option_price)
    print("Future price", future_price)

    option_data = symbol.split('-')
    strike = float(option_data[2])
    call_put = option_data[3]
    expiry = option_data[1]
    calculation_date = option_vol[2]
    term = option_vol[12]

    prices = {'open': 4, 'high': 5, 'low': 6, 'close': 7}

    for price, i in prices.items():

        strike_pct = strike / future_price[i] * 100
        underlying = future_price[i]
        mark_price = option_price[i]
        vol_price = _calc_implied_vol(strike, underlying, calculation_date, expiry, mark_price, call_put)
        # calc_price = _price_option(strike, underlying, calculation_date, expiry, option_vol[i], call_put)

        print(price, "CALCED VOL", vol_price, "VOL", option_vol[i], "price", option_price[i], "future", future_price[i])
        # print(price, "CALCED PRICE", calc_price, "VOL", option_vol[i], "price", option_price[i], "future", strike / option_vol[i+4])
        print(price, "STRIKE", strike_pct, option_vol[i+4])


if __name__ == "__main__":

    exchange = 'deribit'
    symbol = 'BTC/USD:BTC-230512-33000-C'
    exchange_date = '2023-04-28 09:00:00'

    validate_implied_vol(exchange, symbol, exchange_date)