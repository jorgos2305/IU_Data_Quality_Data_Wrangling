import requests
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from shapely.geometry import Point
from typing import List, Dict

from utils.helpers import store, get_url

class EarthQuakeClient:

    def __init__(self):
        self.url = get_url("earthquake")
        self.params = {"method" : "query",
                       "format" : "geojson",
                       "limit" : 100, # Define a limit of 100 eathquakes a day
                       "starttime": None,
                       "endtime" : None,
                       "orderby" : "time"}
        # Ensure that the requests ate performed witih the timezone for germany
        self.berlin_time = ZoneInfo("Europe/Berlin")

    def fetch(self) -> gpd.GeoDataFrame:
        # Get the data from teh last 24 hrs. Use time in germany
        today = datetime.now(self.berlin_time).replace(microsecond=0)
        yesterday = today - timedelta(days=1)
        self.params["starttime"] = yesterday.isoformat()
        self.params["endtime"] = today.isoformat()
        response = requests.get(self.url, params=self.params)
        response.raise_for_status()
        raw_data = response.json()
        # store raw data in the data/raw directory
        store(raw_data, "earthquakes")
        # GeoDataFrame include shapely.Point object for plotting on a map
        records = self._process(raw_data)
        return self._to_geodataframe(records)

    def _process(self, response:Dict) -> Dict[str:List]:
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
        
    def _to_geodataframe(self, records:Dict) -> gpd.GeoDataFrame:
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
    # quick tests
    from pathlib import Path

    client = EarthQuakeClient()
    earthquakes = client.fetch()
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "data" / "raw" / "earthquakes" / "earthquake_test.csv"
    earthquakes.to_csv(output_path, index=False)
    
