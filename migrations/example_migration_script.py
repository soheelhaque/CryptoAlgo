### This is an example of a Database Migration Script;
### It will not run as part of the automated process as it has an invalid filename.
### It can be copied (as can any other migration script) to form the shell of your own script.
###
### To be a valid script, the file must be named:
###
###     migrations/migrate_n_sometext.py
###
### where 'n' is a number (new scripts must be higher number than previous ones)
### and 'sometext' is vaguely description of what the migration does
### Note: The two '_' around the number 'n' are very important!
###

# Import the base class that handles the database connections
from migrations.DatabaseMigrationBaseClass import DatabaseMigration
from datetime import datetime   # datetime is used in this example


# Define your upgrade class that inherits from the base class DatabaseMigration
class MyUpdate(DatabaseMigration):

    def _update_db_version_stamp(self, version, script, comment) -> None:
        # A utility function to insert a row used by this example

        now = datetime.utcnow()

        self.db_cursor.execute(f'''
                INSERT INTO TEST_TABLE
                VALUES(%s, %s, %s, %s); 
                ''',
       (now, version, script, "testing")
       )

        self.db_connection.commit()

    # This is the method that will be executed when this script is run
    # and must be the entry point to the database migration itself.
    def _run_script(self) -> None:
        # This is where you put the code that will be run as part of the upgrade.
        self._update_db_version_stamp(6, "X", "Hi")
        self._update_db_version_stamp(99, "ABC", "There")


# This is required to enable the script to be executed by the automated process
if __name__ == "__main__":
    # Instantiating the migration class causes the script to be run
    MyUpdate()


