import requests
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Tuple
from datetime import datetime

from pipelines.result import ClientResult
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
        project_root = Path(__file__).resolve().parents[1]
        self._datastore = project_root / "data" / "processed" / "datastore.h5"
    
    def fetch(self) -> ClientResult:
        # get the stock data
        response_stocks, metadata, errors = self._fetch_stocks()
        df = self._process(response_stocks)
        return ClientResult(data=df, metadata=metadata, errors=errors)

    def _fetch_stocks(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        response_stocks = []
        metadata = []
        errors = []
        for symbol in self.symbols:
            self.params["symbol"] = symbol
            response = None
            try:
                response = requests.get(self.url, params=self.params, timeout=15)
                response.raise_for_status()
                stocks = response.json().get("Time Series (Daily)")
                if stocks is None:
                    raise KeyError("Time Series (Daily)")
            except requests.HTTPError as e:
                # log error
                errors.append({"timestamp":datetime.now().isoformat(),
                               "url":response.url if response else self.url,
                               "error":str(e),
                               "current_symbol":symbol,
                               "status":response.status_code if response else None})
            except KeyError as ke:
                # log key error
                # if Time Series (Daily) not in response
                # The rate limit for the API has probably been reached
                errors.append({"timestamp":datetime.now().isoformat(),
                               "url":response.url if response else self.url,
                               "error":"Missing key: Time Series (Daily)-ratelimit reached",
                               "current_symbol":symbol,
                               "status":response.status_code if response else None})
            else:
                response_stocks.append({symbol:stocks})
                metadata.append({"fetched_at":datetime.now().isoformat(),
                                "url":response.url if response else self.url,
                                "status":response.status_code if response else None,
                                "success_count":len(response_stocks),
                                "error_count":len(errors)})
        # store raw data
        store(response_stocks, "stocks")
        return response_stocks, metadata, errors
    
    def _process(self, response_stocks:List[Dict]) -> pd.DataFrame:
        # Here, distinguish between two cases
        # 1. The first time storage occurs, we get the 100 days/data points from the API
        # 2. From the 2nd API call on, we only want the last / most recent data point

        if self._datastore.exists():
            with pd.HDFStore(self._datastore, "r") as datastore:
                keys = [
                    key.split(r"/")[-1]
                    for key in datastore.keys()
                    if key.split(r"/")[1] == "stocks"
                    and key.split(r"/")[2] == "data"
                    ]
        else:
            keys = []
        
        dfs = []
        for response in response_stocks: # list
            for symbol, data in response.items(): # of dictionaries
                df = pd.DataFrame(data).T
                df.index = pd.to_datetime(df.index)
                df = df.apply(pd.to_numeric, errors="coerce")
                df = df.rename(columns={"1. open" :"open",
                                        "2. high":"high",
                                        "3. low":"low",
                                        "4. close":"close",
                                        "5. volume": "volume"})
                df["symbol"] = symbol
                df["split_on"] = symbol
                
                if not keys:
                    # data store does not exist yes, use all data point
                    dfs.append(df)
                elif symbol in keys:
                    # data store exists and there is data for a symbol
                    most_recent_data_point = pd.DataFrame(df.iloc[0,:]).T
                    dfs.append(most_recent_data_point)
                else:
                    # the data store exists but no data for a symbol
                    dfs.append(df)
        if not dfs:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "symbol", "split_on"])
        
        df_stocks = pd.concat(dfs, axis=0)
        df_stocks[["open", "high", "low", "close", "volume"]] = df_stocks[["open", "high", "low", "close", "volume"]].astype("float64")
        return df_stocks

if __name__ == "__main__":
    # quick tests
    from pathlib import Path

    client = AlphaVantageClient()
    stocks = client.fetch()
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "data" / "raw" / "stocks" / "stocks_test.csv"
    if stocks.data is not None:
        stocks.data.to_csv(output_path, index=False)