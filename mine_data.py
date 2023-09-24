import os
import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit
from datetime import datetime, timedelta, time
import pytz
import pandas as pd

# Set up the Alpaca Trade API
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
BASE_URL = 'https://paper-api.alpaca.markets'

api = tradeapi.REST(API_KEY, SECRET_KEY, BASE_URL) 


def filter_df_dict(df_dict, start_hr=9, start_min=30, start_sec=0, hours=2):

    market_open_time = pytz.timezone('America/New_York').localize(datetime.now().replace(hour=start_hr, minute=start_min, second=start_sec))
    market_close_time = market_open_time + timedelta(hours=hours)  
    market_open_time -= timedelta(minutes=1)

    # Initialize a dictionary to hold the filtered DataFrames
    filtered_dfs_dict = {}

    # Iterate over each symbol and DataFrame in the dictionary
    for symbol, df in df_dict.items():
        # Ensure the DataFrame index is in datetime format
        df.index = pd.to_datetime(df.index)
        
        # Convert the DataFrame index to New York timezone
        df = df.tz_convert('America/New_York')
        
        # Filter the DataFrame for the first two hours of market open
        filtered_df = df.between_time(market_open_time.time(), market_close_time.time())

        # Store the filtered DataFrame in the new dictionary
        filtered_dfs_dict[symbol] = filtered_df

    return filtered_dfs_dict

def get_stock_symbols(api):
    stocks = api.list_assets(status='active', asset_class='us_equity')
    return [stock.symbol for stock in stocks if stock.tradable]

def partition_list(list, chunk_sz):
    return [list[i:i+chunk_sz] for i in range(0, len(list), chunk_sz)]


def df_dict(api_method, stocks, days, tf=None):

    # Define the time frame
    end_date = datetime.now(pytz.timezone('America/New_York'))
    start_date = end_date - timedelta(days=days)

    stock_partitions = partition_list(stocks, 1000)

    # Define the common parameters
    params = {
        'start': start_date.isoformat(),
        'end': end_date.isoformat(),
        'feed': 'SIP'
    }

    # Check if the method is api.get_bars, and if so, add the timeframe parameter
    if api_method == api.get_bars:
        params['timeframe'] = tf

    all_dfs = []  # List to store individual dataframes

    for stock_partition in stock_partitions:
        try:
            params['symbol'] = stock_partition

            params['symbol'] = ['AAPL'] # DELETE THIS LINE

            # Make the API call with adjusted start and end parameters
            multi_df = api_method(**params).df
            
            all_dfs.append(multi_df)  # Append the dataframe to the list
            print(multi_df)
        except Exception as e:
            print(f"Failed to fetch data for: {e}")
        break # DELETE THIS LINE
    # Concatenate all the individual dataframes into a large dataframe
    large_df = pd.concat(all_dfs)

    # Group by the 'symbol' column and create a dictionary of dataframes
    data_frames = {symbol: df for symbol, df in large_df.groupby('symbol')}

    return data_frames

def compute_perc_df_dict(df_dict):
    # Initialize a new dictionary to store the modified DataFrames
    modified_dfs_dict = {}

    # Iterate over each symbol and DataFrame in the dictionary
    for symbol, df in df_dict.items():
        # Calculate the percentage change in the 'close' column
        df['percent_change'] = df['close'].pct_change() * 100  # multiply by 100 to get percentage
    
        # Store the modified DataFrame in the new dictionary
        modified_dfs_dict[symbol] = df

    return modified_dfs_dict

stocks = get_stock_symbols(api)
minute_dfs_dict = df_dict(api.get_bars, stocks, 1, TimeFrame(1, TimeFrameUnit.Minute))
filtered_minute_dfs_dict = filter_df_dict(minute_dfs_dict)
minute_perc_dfs_dict = compute_perc_df_dict(filtered_minute_dfs_dict)

# Assuming data_frames is your dictionary with symbols as keys and DataFrames as values
# for symbol, df in minute_perc_dfs_dict.items():
#     print("Symbol:", symbol)
#     print("DataFrame:")
#     print(df)
#     print("\n" + "="*80 + "\n")  # printing a separator for better readability


trades_dfs_dict = df_dict(api.get_trades, stocks, 1)

for symbol, df in trades_dfs_dict.items():
    print("Symbol:", symbol)
    print("DataFrame:")
    print(df)
    print("\n" + "="*80 + "\n")  # printing a separator for better readability

