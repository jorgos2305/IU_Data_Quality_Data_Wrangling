import requests
import os
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict

from utils.helpers import get_url, load_alpha_vantage_symbols, store

load_dotenv("config/.env")

class AlphaVantageClient:
    # This API has a rate limit of 25 calls a day so be patient
    def __init__(self) -> None:
        self.url = get_url("stocks")
        if self.url is None:
            # log error
            raise ValueError("URL not found in sources.csv")
        self.apikey = os.getenv("ALPHAVANTAGE_API_KEY")
        self.symbols = load_alpha_vantage_symbols()
        self.params = {"function" : "TIME_SERIES_DAILY", # this endpoint provides a daily time series of the equity specified
                       "symbol" : None, # the equity -> replace this by all the companies that should be followed
                       "outputsize" : "compact",
                       "datatype" : "json",
                       "apikey" : self.apikey}
    
    def fetch(self) -> pd.DataFrame:
        # get the stock data
        response_stocks = self._fetch_stocks()
        # process the resonse
        #return the DatFrame
        return self._process(response_stocks)

    def _fetch_stocks(self) -> List[Dict]:
        response_stocks = []
        for symbol in self.symbols[:2]:
            self.params["symbol"] = symbol
            try:
                response = requests.get(self.url, params=self.params)
                response.raise_for_status()
                stocks = response.json()["Time Series (Daily)"]
            except requests.HTTPError:
                # log error
                pass
            except KeyError as ke:
                # log key error
                # if Time Series (Daily) not in response
                # The rate limit for the API has probably been reached
                pass
            else:                
                response_stocks.append({symbol:stocks})
        # store raw data
        store(response_stocks, "stocks")
        return response_stocks
    
    def _process(self, response_stocks:List[Dict]) -> pd.DataFrame:
        # Here, distinguish between two cases
        # 1. The first time storage occurs we get the 100 days/data points from the API
        # 2. From the 2nd API call on, we only want the last / most recent data point
        # This cases are handles by the DataStoreClass, since it is part of the storage process
        dfs = [] # Store all dataframes for later concatenation
        for response in response_stocks: # list
            for symbol, data in response.items(): # of dictionaries
                df = pd.DataFrame(data).T
                df.index = pd.to_datetime(df.index)
                df = df.apply(pd.to_numeric, errors="coerce")
                df = df.rename(columns={"1. open" :"open", "2. high":"high", "3. low":"low", "4. close":"close", "5. volume": "volume"})
                df["symbol"] = symbol
                df["split_on"] = df["symbol"]
                dfs.append(df)
        return pd.concat(dfs, axis=0)

if __name__ == "__main__":
    # quick tests
    from pathlib import Path

    client = AlphaVantageClient()
    stocks = client.fetch()
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "data" / "raw" / "stocks" / "stocks_test.csv"
    stocks.to_csv(output_path, index=False)