from datetime import datetime, date, timedelta
import psycopg2


class QuestDBDataBundle():
    """ Class for initializing a 'data bundle' from QuestDB table
    """

    bundle_name = None
    data_start = None
    data_end = None
    df = None

    def __init__(self, bundle_name: str):
        """ By default, a bundle will be initialised with all the data available. optionally, constraints can
            be put on the query to limit the size of the dataset.

            :param bundle_name: arbitrary name given to identify the data bundle
            :param data_start: optional start date for earliest date when bundle data will begin
            :param data_end: optional end date for latest date when bundle data will end
            :param exchange: optional exchange name (or list of exchange names) to limit the bundle content

        """

        self.bundle_name = bundle_name

        db_user = 'admin'
        db_password = 'quest'
        db_host = '127.0.0.1'
        db_port = 8812
        db_name = 'qdb'
        self.db_connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port,
                                         database=db_name)
        self.db_cursor = self.db_connection.cursor()

        self.query_string = None

        self.ohlcv = self._load_data_table(self.db_cursor,
                                        self.bundle_name)

    def _load_data_table(self, cursor: psycopg2.extensions.cursor,
                         bundle_name: str):

        columns = "Exchange, MarketSymbol, ExchangeDate, ExchangeTimestamp, open, high, low, close, volume"

        self.query_string = f"select {columns} from {bundle_name}"
        self.db_cursor.execute(self.query_string)
        result = cursor.fetchall()

        return result

    def symbols(self):

        symbols = set()
        for row in self.ohlcv:
            symbols.add(row[1])

        return symbols

    def row_does_not_exist(self, ohlcv_row) -> bool:

        self.db_cursor.execute(f'''SELECT *
                                        FROM '{self.bundle_name}'
                                        WHERE MarketSymbol = %s
                                        AND Exchange = %s
                                        AND ExchangeTimestamp = %s;
                                        ''',
                               (ohlcv_row[1], ohlcv_row[0], ohlcv_row[3]))
        record = self.db_cursor.fetchone()

        # if record is None:
        #     print("DOES NOT EXIST", record, record is None)
        # else:
        #     print("*** EXISTING RECORD***")

        return record is None

    def insert_row(self, ohlcv_row):

        if self.row_does_not_exist(ohlcv_row):
            now = datetime.utcnow()
            self.db_cursor.execute(f'''
                        INSERT INTO {self.bundle_name}
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        ''',
                           (now, ohlcv_row[0], ohlcv_row[1], ohlcv_row[2], ohlcv_row[3], ohlcv_row[4], ohlcv_row[5],
                            ohlcv_row[6], ohlcv_row[7], ohlcv_row[8]))

            self.db_connection.commit()
            # print("ROWCOUNT", self.db_cursor.rowcount)
            return True
        return False

    def close(self):

        if self.db_cursor:
            self.db_cursor.close()
        if self.db_connection:
            self.db_connection.close()


def add_symbol_data_to_history_table(symbol, data_bundle_ohlcv, data_bundle_history):

    i = 0
    added = 0
    existing = 0

    for row in data_bundle_ohlcv.ohlcv:
        i += 1
        # if i % 10000 == 0:
        #     print(i, row)

        if symbol == row[1]:
            # print("ADD ROW", symbol, row)
            if data_bundle_history.insert_row(row):
                added += 1
            else:
                existing += 1

    print("PROCESSED", symbol, "ADDED", added, "EXISTING", existing)

if __name__ == "__main__":

    # get data and symbols in existing price history table
    data_bundle_ohlcv = QuestDBDataBundle('OHLCV')
    ohlcv_symbols = data_bundle_ohlcv.symbols()

    # get data and symbols in historic price history table
    data_bundle_history = QuestDBDataBundle('DERIBIT_OHLCV_COPY')
    ohlcv_history_symbols = data_bundle_history.symbols()

    print("OHLCV", len(ohlcv_symbols), len(data_bundle_ohlcv.ohlcv))
    print("OHLCV HISTORY", len(ohlcv_history_symbols), len(data_bundle_history.ohlcv))

    # print("INTERSECTION", len(ohlcv_symbols.intersection(ohlcv_history_symbols)))
    #
    # print("To Add TO History", len(ohlcv_symbols - ohlcv_history_symbols))
    # # #
    # # # for i in ohlcv_symbols - ohlcv_history_symbols:
    # # #     print(i)
    # #
    # # get symbols of price table to add to history table
    # to_add = ohlcv_symbols - ohlcv_history_symbols

    for symbol in list(ohlcv_symbols):
        add_symbol_data_to_history_table(symbol, data_bundle_ohlcv, data_bundle_history)

    data_bundle_ohlcv.close()
    data_bundle_history.close()

