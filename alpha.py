
import urllib3
from urllib3 import HTTPSConnectionPool
import json
import pandas as pd
import csv
from io import BytesIO

API_HOST_NAME = 'www.alphavantage.co'
API_KEY  = 'demo'
INTERVALS = [
    '1min',
    '5min',
    '15min',
    '30min',
    '60min'
]

OUTPUT_SIZE = [
    'compact',
    'full'
]
API_FREQ = [
  'DAILY',
  'MONTHLY',
  'WEEKLY',
]

TECH_INDS = [
  'SMA',
  'EMA',
  'VWAP'
  #Add list of indicators available
]
SERIES_TYPE = [
  'close', 
  'open', 
  'high', 
  'low'
]

class StockScreener:
  def __init__(self):
     self.session = HTTPSConnectionPool(API_HOST_NAME)

  def format_columns(self,columns):
    '''
    Process Column Names
    :param1 Dataframe Columns
    returns list of formatted columns after stripping sequence
    '''
    return list(map(lambda col: col[3:],columns))


  def get_request(self,payload):
    '''
    Apply Get Request
    :param Payload with Method and query parameters
    :returns: raw data in bytes
    '''    

    r = self.session.request('GET','/query',fields=payload);
    
    return r.data;

  def get_symbols_match(self,keywords):
    '''
    fetch symbol results based on keyword provided
    :param1 function: Symbol search
    :returns: Symbol search result in JSON format
    Example: https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords=tesco&apikey=demo
    '''    
    payload = {
      'function': 'SYMBOL_SEARCH',
      'keywords': keywords,
      'apikey': API_KEY
    }
    
    return json.loads(self.get_request(payload))


  def get_intraday_prices(self,symbol,interval,adjusted='true',outputsize='compact'):


      '''
      Get historical data for a stock
      :param1 symbol: exact EPIC
      :param2 interval: Time Series Interval
      :param3 output: ammount of data returned (full, compact)
      :returns: a list of historical stock quotes in json format
      Example URL: https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo
      '''

      assert interval in INTERVALS
      assert outputsize in OUTPUT_SIZE

      payload = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval' : interval,
        'output_type' : outputsize,
        'apikey': API_KEY
      }
     
      return json.loads(self.get_request(payload))

  def get_intraday_prices(self,symbol,interval,adjusted='true',outputsize='compact'):

      '''
      Get historical data for a stock
      :param1 symbol: exact EPIC
      :param2 interval: Time Series Interval4
      :param3 adjusted: Adjust dividend yield
      :param4 output: ammount of data returned (full, compact)
      :returns: a list of historical stock quotes in json format
      Example URL: https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo
      '''

      assert interval in INTERVALS
      assert outputsize in OUTPUT_SIZE

      payload = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'interval' : interval,
        'output_type' : outputsize,
        'apikey': API_KEY
      }
     
      return json.loads(self.get_request(payload))

  def get_historical_prices(self,symbol,freq,datatype='json'):

      '''
      Get historical data for a stock
      :param1 symbol: exact EPIC
      :param2: Frequency daily/weekly/monthly
      :param3: output data type format
      :returns: dataframe
      Example URL: https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=IBM&apikey=demo
      '''

      assert freq in API_FREQ

      function = '_'.join(['TIME_SERIES',freq])

      payload = {
        'function': function,
        'symbol': symbol,
        'datatype': datatype,
        'apikey': API_KEY
        
      }

      return self.get_request(payload);


  def get_current_quote(self, symbol,datatype='json'):
      '''
      Get current price and volume
      :param1 symbol: a stock symbol
      :param2 datatype: Output format of either json or csv
      :returns: realtime Price/Volume information of the given symbol in json format
      Example: https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey=demo
      '''

      payload = {
          'function': 'GLOBAL_QUOTE',
          'symbol': symbol,
          'datatype': datatype,
          'apikey': API_KEY
      }

      return self.get_request(payload);
        
  def get_technical_indicator(self, function, symbol,interval,time_period,series_type,datatype='json'):
      '''
      Get current price and volume
      :param1 function: Technical Indicator
      :param2 symbol: a stock symbol
      :param3 interval: Frequency
      :param4 time_period: Positive Integer range
      :param5 series_type: SERIES_TYPE
      :param6 datatype: Output format of either json or csv
      :returns: Technical Indicator data
      Example: https://www.alphavantage.co/query?function=SMA&symbol=IBM&interval=weekly&time_period=10&series_type=open&apikey=demo
      '''

      assert (interval in INTERVALS or interval in ['daily','weekly','monthly'])
      assert function in TECH_INDS
      assert series_type in SERIES_TYPE
      assert time_period > 0

      payload = {
          'function': function,
          'symbol': symbol,
          'interval': interval,
          'time_period': str(time_period),
          'series_type': series_type,
          'datatype': datatype,
          'apikey': API_KEY
      }

      return self.get_request(payload);


sample = StockScreener()

#Keyword Search
results = sample.get_symbols_match('VOD')
df_results = pd.DataFrame(list(results.values())[0])
df_results.columns = sample.format_columns(df_results.columns)

print(df_results)

#Intraday historical Prices
results = sample.get_intraday_prices('VOD','5min')
df_results = pd.DataFrame(results["Time Series (5min)"])
df_results = df_results.T
df_results.columns = sample.format_columns(df_results.columns)

print(df_results)

#Daily/Monthly/Weekly Prices
datatype = 'json'
raw_results = sample.get_historical_prices('IBM','DAILY',datatype)

if(datatype == 'csv'):
  df_results = pd.read_csv(BytesIO(raw_results));
  df_results.set_index('timestamp',inplace=True)
else:
  df_results = pd.DataFrame(json.loads(raw_results))

print(df_results)

# Get Latest Price / Volume
datatype = 'csv'
raw_results = sample.get_current_quote('IBM',datatype)

if(datatype == 'csv'):
  df_results = pd.read_csv(BytesIO(raw_results));

else:
  df_results = pd.DataFrame(json.loads(raw_results))
  df_results = df_results.T
  df_results.columns = sample.format_columns(df_results.columns)

df_results.set_index('symbol',inplace=True)

print(df_results)

# technical Indicator

# Get Latest Price / Volume
#https://www.alphavantage.co/query?function=SMA&symbol=IBM&interval=weekly&time_period=10&series_type=open&apikey=demo
datatype = 'json'
raw_results = sample.get_technical_indicator('SMA','IBM','weekly',10,'open',datatype)

if(datatype == 'csv'):
  df_results = pd.read_csv(BytesIO(raw_results));
else:
  df_results = pd.DataFrame(json.loads(raw_results))

print(df_results)

