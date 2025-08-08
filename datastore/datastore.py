import pandas as pd
from pathlib import Path
from typing import List, Dict

from pipelines.result import ClientResult

class DataStore:

    def __init__(self):
        self.store_path = Path(__file__).resolve().parents[1].joinpath("data/processed/datastore.h5")
        self._default_min_itemsize = 15
    
    def _update_min_itemsize(self, df:pd.DataFrame|None) -> int:
        # to avoid min_size problems when storing string columns
        min_item_size = self._default_min_itemsize
        if df is not None and not df.empty:
            for col in df.select_dtypes(include=["object", "string"]).columns:
                entry_len = df[col].astype(str).map(len).max()
                min_item_size = max(self._default_min_itemsize, entry_len)
        return min_item_size

    
    def store(self, client:str, result:ClientResult, split_on:str):
        if not client:
            raise ValueError(f"Invalid name for client: {client}")
        if not split_on:
            raise ValueError(f"Invalid column name for splitting: {split_on}")
        
        min_itemsize = self._default_min_itemsize
        if result.data is None:
            min_itemsize = max(min_itemsize, self._update_min_itemsize(result.data))
        if result.metadata is not None:
            min_itemsize = max(min_itemsize, self._update_min_itemsize(pd.DataFrame(result.metadata)))
        if result.errors is not None:
            min_itemsize = max(min_itemsize, self._update_min_itemsize(pd.DataFrame(result.errors)))
        
        
        with pd.HDFStore(self.store_path, "a") as datastore:
            # Make sure there is a column on which it can be grouped on
            if result.data is not None and not result.data.empty:
                if split_on not in result.data.columns:
                    raise ValueError(f"{split_on} not found in Dataframe columns: {result.data.columns}")
        
                for group, data in result.data.groupby(split_on):
                    datastore.append(f"{client}/data/{group}", data, format="table", data_columns=True, min_itemsize=min_itemsize)
            
            if result.metadata is not None and len(result.metadata) > 0:
                metadata_df = pd.DataFrame(result.metadata)
                datastore.append(f"{client}/metadata", metadata_df, format="table", data_columns=True, min_itemsize=min_itemsize)

            if result.errors is not None and len(result.errors) > 0:
                errors_df = pd.DataFrame(result.errors)
                datastore.append(f"{client}/errors", errors_df, format="table", data_columns=True, min_itemsize=min_itemsize)


if __name__ == "__main__":

    store = DataStore()
    print(store.store_path)