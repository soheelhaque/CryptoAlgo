### A Database Migration Script ###
import tomli
import psycopg2
from abc import ABC, abstractmethod


class DatabaseMigration(ABC):
    """ Abstract Base Class for creating database migrations
        Migration scripts should derive from this class and implement the '_run_scripts' method.

        # Example Skeleton Migration Script

        from migrations.MigrationScriptBaseClass import DatabaseMigration

        class My_Migration(DatabaseMigration):

            def _run_script():
                # do some data-basey stuff e.g.

                self.db_cursor.execute(f'''
                    CREATE TABLE TEST_COLUMN AS (
                    select ts, date_trunc('day', ts) as Day, Version, Script, Comment from TEST_TABLE
                    ) timestamp(ts);
                    DROP TABLE TEST_TABLE;
                    RENAME TABLE TEST_COLUMN TO TEST_TABLE;
                    '''
                )

                self.db_connection.commit()


        # Add this line to execute the script
        if __name__ == "__main__":

            My_Migration()


    """

    def __init__(self):

        self.config = "MigrateDatabase.toml"

        self.db_config: dict = self._load_config()
        self.db_cursor = None
        self.db_connection = None

        try:
            self._connectDB(self.db_config)
            # implement this method with migration code
            self._run_script()
        except Exception as e:
            print(f"An exception has occurred: {e}")
        finally:
            if self.db_cursor:
                self.db_cursor.close()
            if self.db_connection:
                self.db_connection.close()

    def _load_config(self) -> dict:
        """ Load up the database configuration .toml file
        """

        db_config = {}

        with open(self.config, mode="rb") as cf:
            config = tomli.load(cf)

            # database config
            db_config['user'] = config['database']['user']
            db_config['password'] = config['database']['password']
            db_config['host'] = config['database']['host']
            db_config['port'] = config['database']['port']
            db_config['database'] = config['database']['database']

        return db_config

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

    @abstractmethod
    def _run_script(self) -> None:
        """ Code to be executed against the database
        """
        pass



