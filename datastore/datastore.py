import pandas as pd
from pathlib import Path
from typing import List, Dict

from pipelines.result import ClientResult

class DataStore:

    def __init__(self):
        self.store_path = Path(__file__).resolve().parents[1].joinpath("data/processed/datastore.h5")
    
    def store(self, client:str, result:ClientResult, split_on:str):
        # Make sure there is a column on which it can be grouped on
        if split_on not in result.data.columns:
            raise ValueError(f"{split_on} not found in Dataframe columns: {result.data.columns}")
        
        # to avoid min_size problems when storing string columns
        for col in result.data.select_dtypes(include=["object", "string"]).columns:
            entry_len = result.data[col].astype(str).map(len).max()  
            min_itemsize = max(entry_len, 15) # if the column is empty, use this as the default length

        with pd.HDFStore(self.store_path, "a") as datastore:
            for group, data in result.data.groupby(split_on):
                datastore.append(f"{client}/data/{group}", data, format="table", data_columns=True, min_itemsize=min_itemsize)
            
            if result.metadata is not None:
                metadata_df = pd.DataFrame(result.metadata)
                datastore.append(f"{client}/metadata", metadata_df, format="table", data_columns=True, min_itemsize=min_itemsize)

            if result.errors is not None:
                errors_df = pd.DataFrame(result.errors)
                datastore.append(f"{client}/errors", errors_df, format="table", data_columns=True, min_itemsize=min_itemsize)


if __name__ == "__main__":

    store = DataStore()
    print(store.store_path)