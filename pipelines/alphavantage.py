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
                       "dataype" : "json",
                       "apikey" : self.apikey}
    
    def fetch(self) -> pd.DataFrame:
        # get the stock data
        response_stocks = self._fetch_stocks()
        # process the resonse
        #return the DatFrame
        return self._process(response_stocks)

    def _fetch_stocks(self) -> List[Dict]:
        response_stocks = []
        for symbol in self.symbols:
            self.params["symbol"] = symbol
            try:
                response = requests.get(self.url, params=self.params)
                response.raise_for_status()
            except requests.HTTPError:
                # log error
                pass
            else:
                store(response.json())
                # Time Series (Daily) is not present for every symbol???
                stocks = response.json()["Time Series (Daily)"]
                response_stocks.append({symbol:stocks})
        store(response_stocks)
        return response_stocks
    
    def _process(self, response_stocks:List[Dict]) -> pd.DataFrame:
        # Here, distinguish between two cases
        # 1. The first time storage occurs we get the 100 days/data points from the API
        # 2. From the 2nd API call on, we only want the last / most recent data point
        # This cases are handles by the DataStoreClass, since it is part of the storage process
        dfs = [] # Store all dataframes for later concatenation
        for response in response_stocks:
            symbol = response["symbol"].keys()[0]
            data = response["symbol"]
            df = pd.DataFrame(data).T
            df.index = pd.to_datetime(df.index, day_first=False)
            df = df.apply(pd.to_numeric, erros="coerce")
            df = df.rename(columns={"1. open" :"open", "2. high":"high", "3. low":"low", "4. close":"close", "5. volume": "volume"})
            df["symbol"] = symbol
            dfs.append(df)
        return pd.concat(dfs, axis=1)

if __name__ == "__main__":
    # quick tests
    from pathlib import Path

    client = AlphaVantageClient()
    stocks = client.fetch()
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "data" / "raw" / "stocks" / "stocks_test.csv"
    stocks.to_csv(output_path, index=False)