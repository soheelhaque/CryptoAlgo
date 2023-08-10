
<img src="https://dequantifi.com/wp-content/uploads/2022/04/dq_web_logo-1.png" width="150" height="50" style="vertical-align:bottom">

# CryptoAlgo
This project contains the code for the DeQuantifi© CCXT batch feed that takes historical prices (actually 1 day OHLCV candles) and feeds them into the QuestDB time series database.
It also has the code to update missing history by directly using the Deribit Historic Price API.
And it will update the Vol historic data as well, by using the price history to imply vols for open/close.

## Getting Started
1. First, install the QuestDB timeseries database on your computer. We recommend a Docker installation (see [here](https://questdb.io/docs/get-started/docker)). If you wish, you can install binaries for your OS or Homebrew (see [here](https://questdb.io/docs/#get-started)).
2. Install Python 3.8.6 or any later version of Python. Ensure that python is on your system path. Also ensure that pip is installed.
3. Next, clone the Git project onto your local computer
4. (Advanced users may want to create a new virtual environment before this step). Install the Python requirements by running the following from the project directory (the directory containing requirements.txt):

        python -m pip install -r requirements.txt

5. The file CryptoPriceDBGateway.toml contains the config. You will need to edit to suit your OS (Windows and Posix config are included). The "CCXT" section defines which exchanges and which markets are retrieved using [CCXT](https://docs.ccxt.com/en/latest/manual.html) descriptors. The "database" section defines the database login details, and "logging" defines the location and threshold level for log files
6. You can now simply run the script and your QuestDB database will be populated with daily historical price and vol data from the exchange. The command line output and the log file show you what data has been written to the database. To run the script, type the following from the project directory:

        python CryptoPriceDBGateway.py

7. After running that script, you need to run this one:
   
        python DeribitVolHistoryUpdate.py

The first script updates the prices, the second updates the vols.

# Filling in Historic Price Data
It is known that historic products (ie those that have already expired) do not get included within the CCXT historic price feed.
That feed only includes history for products that are still tradeable.
This is mostly an issue for Deribit, where we have lots of options and futures that expire all the time.
The 'back fill' of deribit data for products that are already expired may be required if the above script is not run every day or
there is a need to initialise a new database with historic data.

This script is meant to only be run once, at initialisation, or exceptionally if there is a gap in the live price feed above.

      python DeribitPriceHistoryDBGateway.py

It can take a while to run, and has 'softwired' parameters to limit the historic dates it looks at.

If this script needs to be run, then it is most likely that the following Vol History one will need to be run as well.
Though, it will be run anyway, the next time there is a normal daily price update.

# Filling in Historic Vol data
As well as price history, the database also contains Vol history; this table is populated by running
a script that examines what option data is in the price table that does not have corresponding
implied vol data in the vol history table. 
This script needs to run daily, after the price history or Deribit missing data scripts has been run.

      python DeribitVolHistoryUpdate.py


