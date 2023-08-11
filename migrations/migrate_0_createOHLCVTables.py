### Database Migration Script;

# Import the base class that handles the database connections
from migrations.DatabaseMigrationBaseClass import DatabaseMigration

# Define your upgrade class that inherits from the base class DatabaseMigration
class MyUpdate(DatabaseMigration):

    # This is the method that will be executed when this script is run
    # and must be the entry point to the database migration itself.
    def _run_script(self) -> None:
        # This is where you put the code that will be run as part of the upgrade.
        self._create_OHLCV_table()
        self._create_OHLCV_VOL_table()

    def _create_OHLCV_table(self):

        self.db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS 'OHLCV' (
                                ts TIMESTAMP,
                                Exchange  STRING NOT NULL,
                                MarketSymbol  STRING NOT NULL,
                                ExchangeDay TIMESTAMP NOT NULL,
                                ExchangeDate TIMESTAMP NOT NULL,
                                ExchangeTimestamp LONG NOT NULL,
                                Open  FLOAT,
                                High  FLOAT,
                                Low   FLOAT,
                                Close FLOAT,
                                Volume  FLOAT
                        ) timestamp(ts);''')

        self.db_connection.commit()

    def _create_OHLCV_VOL_table(self):

        self.db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS OHLCV_VOL (
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

        self.db_connection.commit()



# This is required to enable the script to be executed by the automated process
if __name__ == "__main__":
    # Instantiating the migration class causes the script to be run
    MyUpdate()


