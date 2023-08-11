### Module to run to ensure database is updated to latest version
import tomli
from datetime import datetime
import psycopg2
import glob


class MigrateDatabase():

    def __init__(self):
        self.version_table = "DB_VERSION"
        self.config = "MigrateDatabase.toml"
        self._migration_path = 'migrations/'

        self.db_config: dict = self._load_config()
        self.db_cursor = None
        self.db_connection = None

        try:
            self._connectDB(self.db_config)
            self._check_db_version_table_exists()
            self.do_migrations()
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

    def _check_db_version_table_exists(self) -> None:
        """ IF Vol History table does not exist, then create it
        """

        self.db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.version_table} (
                                    ts TIMESTAMP,
                                    Version FLOAT NOT NULL,
                                    Script STRING NOT NULL,
                                    Comment STRING NULL
                            ) timestamp(ts);''')

    def _get_migrations_in_order(self) -> dict:
        """ Returns a list of migration files in version order
        """

        migration_files = glob.glob(self._migration_path + 'migrate_*_*.py')

        file_order = {float(file_name.split('/migrate_')[1].split('_')[0]): file_name for file_name in migration_files}

        return dict(sorted(file_order.items()))

    def _get_db_version(self) -> float:
        """ Get current version of database
        """

        self.db_cursor.execute(f'''SELECT max(Version) FROM '{self.version_table}';''')

        last_update_version = self.db_cursor.fetchone()

        # print(last_update_version)

        return last_update_version[0]

    def _update_db_version_stamp(self, version: float, script: str) -> None:
        """ Append a new record to the version history
        """

        now = datetime.utcnow()

        self.db_cursor.execute(f'''
                INSERT INTO {self.version_table}
                VALUES(%s, %s, %s, %s); 
                ''',
       (now, version, script, "")
       )

        self.db_connection.commit()

    def _apply_migration_script(self, version: float, script: str) -> None:
        """ Execute the script for this version upgrade
        """
        # execute script
        with open(script) as f:
            exec(f.read())

        # update the database
        self._update_db_version_stamp(version, script)

    def do_migrations(self) -> None:
        """ Iterate through migration scripts, applying any that have not been applied
        """
        # identify the scripts that could be run, in the correct order
        migration_files = self._get_migrations_in_order()

        # print("MIGRATION FILES", migration_files)

        db_version = self._get_db_version()
        print("CURRENT DB VERSION", db_version, "CHECKING FOR UPDATES...")

        # iterate through scripts, determine if any have not been applied
        for version, script in migration_files.items():

            # if script not applied, apply it and update version table
            if db_version is None or version > float(db_version):
                print("DB UPGRADE FOUND: APPLYING UPGRADE TO VERSION", version, "WITH SCRIPT", script.split('/')[1])
                self._apply_migration_script(version, script)
            else:
                # print("SKIPPING VERSION", version)
                pass

        new_db_version = self._get_db_version()

        if new_db_version != db_version:
            print("DB UPGRADED FROM VERSION", db_version, "TO VERSION", new_db_version)
        else:
            print("NO UPGRADES FOUND. DATABASE IS UP TO DATE", new_db_version)


if __name__ == "__main__":
    # Execute migrations
    migrate_database = MigrateDatabase()

