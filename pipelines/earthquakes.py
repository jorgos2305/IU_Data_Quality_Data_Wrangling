import requests
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta, timezone
from shapely.geometry import Point

from utils.helpers import store, get_url

class EarthQuakeClient:

    def __init__(self, url:str):
        self.url = url
        # use to get the data from the last 24 hrs
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        self.params = {"method" : "query",
                       "format" : "geojson",
                       "limit" : 100, # Define a limit of 100 eathquakes a day
                       "starttime": yesterday.isoformat(),
                       "endtime" : now.isoformat(),
                       "orderby" : "time"}

    def fetch(self):
        # get data from API
        response = requests.get(self.url, params=self.params)
        response.raise_for_status()
        raw_data = response.json()
        # store raw data in the data/raw directory
        store(raw_data, "earthquake")
        # GeoDataFrame include shapely.Point object for plotting on a map
        records = self._process(raw_data)
        return self._to_geodataframe(records)

    def _process(self, response:dict) -> dict[str:list]:
        # helps process the requested data before creating the data frame
        # The kekys match the keys return by the API for better processing
        records = {"time":[],
                   "mag":[],
                   "magType":[],
                   "alert":[],
                   "tsunami":[],
                   "place":[],
                   "coordinates":[]}
        # the response contains the information about the earthquakes under the features key
        for earthquake in response["features"]:
            # Using the keys of the records dict get the values from each earthquake
            for feature in records:
                if feature in earthquake["properties"]:
                    records[feature].append(earthquake["properties"][feature])
                # The coordinates of the earthquake are located under the "geometry" key of the response
                else:
                    records[feature].append(earthquake["geometry"][feature])
        return records
        
    def _to_geodataframe(self, records:dict):
        # converts data into geodataframe
        df = pd.DataFrame(records)
        # separte longitude and latitude from the depth
        df["geometry"] = df.coordinates.apply(lambda coord: Point(coord[:2]))
        df["depth"] = df.coordinates.apply(lambda coord: coord[-1])
        df = df.drop("coordinates", axis=1)
        # rename columns for better understanding
        df = df.rename(columns={"time":"timestamp", "mag":"magnitude", "magType":"scale"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    
if __name__ == "__main__":
    # some manual tests :)
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parent.parent))

    url = get_url("earthquake")
    earthquake = EarthQuakeClient(url).fetch()
    
