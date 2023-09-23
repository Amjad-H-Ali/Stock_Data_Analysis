import os
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
import pytz
import pandas as pd

# Set up the Alpaca Trade API
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
BASE_URL = 'https://paper-api.alpaca.markets'

api = tradeapi.REST(API_KEY, SECRET_KEY, BASE_URL) 

def get_stock_symbols(api):
    stocks = api.list_assets(status='active', asset_class='us_equity')
    return [stock.symbol for stock in stocks if stock.tradable]

def partition_list(list, chunk_sz):
    return [list[i:i+chunk_sz] for i in range(0, len(list), chunk_sz)]


def get_minute_bars_df(api, stocks, days):

    # Define the time frame
    end_date = datetime.now(pytz.timezone('America/New_York'))
    start_date = end_date - timedelta(days=days)

    stock_partitions = partition_list(stocks, 1000)

    all_dfs = []  # List to store individual dataframes

    for stock_partition in stock_partitions:
        try:
            # Get minute bars for each day
            multi_df = api.get_bars(symbol=stock_partition, timeframe=tradeapi.rest.TimeFrame.Minute,
                                                start=start_date.date().isoformat(),
                                                end=end_date.date().isoformat()).df

            all_dfs.append(multi_df)  # Append the dataframe to the list

        except Exception as e:
            print(f"Failed to fetch data for: {e}")

    # Concatenate all the individual dataframes into a large dataframe
    large_df = pd.concat(all_dfs)

    # Group by the 'symbol' column and create a dictionary of dataframes
    data_frames = {symbol: df for symbol, df in large_df.groupby('symbol')}

    return data_frames


stocks = get_stock_symbols(api)

minute_dfs = get_minute_bars_df(api, stocks, 1)

apple_data = minute_dfs['AAPL']
# Define the specific date and time you are interested in
timestamp = pd.Timestamp('2023-09-21 09:30:00', tz='America/New_York')

# Use the loc indexer to retrieve the bar for the specified timestamp
bar_at_timestamp = apple_data.loc[timestamp]
bar_at_next_timestamp = apple_data.shift(-1).loc[timestamp]
print(bar_at_timestamp)
print(bar_at_next_timestamp)

# Assuming data_frames is your dictionary with symbols as keys and DataFrames as values
for symbol, df in minute_dfs.items():
    print("Symbol:", symbol)
    print("DataFrame:")
    print(df)
    print("\n" + "="*80 + "\n")  # printing a separator for better readability




