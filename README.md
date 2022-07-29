
<img src="https://dequantifi.com/wp-content/uploads/2022/04/dq_web_logo-1.png" width="150" height="50" style="vertical-align:bottom">

# CryptoAlgo
This project contains the code for the DeQuantifi© DQAlgo Backtesting and Algo Trading framework for cryptocurrencies. Today, it contains the code for the CCXT batch feed that takes historical prices (actually 1 day OHLCV candles) and feeds them into the QuestDB time series database.

## Getting Started
1. First, install the QuestDB timeseries database on your computer. We recommend a Docker installation (see [here](https://questdb.io/docs/get-started/docker)). If you wish, you can install binaries for your OS or Homebrew (see [here](https://questdb.io/docs/#get-started)).
2. Install Python 3.8.6 or any later version of Python. Ensure that python is on your system path. Also ensure that pip is installed.
3. Next, clone the Git project onto your local computer
4. (Advanced users may want to create a new virtual environment before this step). Install the Python requirements by running the following from the project directory (the directory containing requirements.txt):

        python -m pip install -r requirements.txt

5. The file CryptoPriceDBGateway.toml contains the config. Edit as you see fit. The "CCXT" section defines which exchanges and which markets are retrieved using [CCXT](https://docs.ccxt.com/en/latest/manual.html) descriptors. The "database" section defines the database login details, and "logging" defines the location and threshold level for log files
6. You can now simply run the script and your QuestDB database will be populated with daily historical data from the exchange. The command line output and the log file show you what data has been written to the database. To run the script, type the following from the project directory:

        python CryptoPriceDBGateway.py
