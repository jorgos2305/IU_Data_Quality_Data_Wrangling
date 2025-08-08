import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Dict

from pipelines.result import ClientResult
from utils.helpers import store, get_url

class EarthQuakeClient:

    MAGNITUDE_TYPE_DESCRIPTION = {"Mw": "Moment Magnitude",
                                  "Ms": "Surface Wave Magnitude",
                                  "mb": "Body Wave Magnitude",
                                  "ml": "Local (Richter) Magnitude",
                                  "mb_lg": "Lg-Wave Magnitude",
                                  "md": "Duration Magnitude",
                                  "MH": "Hand-calculated Magnitude",
                                  "MI": "Intensity-derived Magnitude",
                                  "Me": "Energy Magnitude",
                                  "Mg": "Surface Wave from Ground Displacement",
                                  "MWb": "Moment Magnitude from Body Waves",
                                  "Mwr": "Regional Moment Magnitude",
                                  "MwC": "Centroid Moment Magnitude",
                                  "MwB": "Body-wave Derived Moment Magnitude",
                                  "mww": "Moment Magnitude from W-phase"}

    def __init__(self) -> None:
        self.url = get_url("earthquake")
        self.params = {"method" : "query",
                       "format" : "geojson",
                       "limit" : 100, # Define a limit of 100 eathquakes a day
                       "starttime": None,
                       "endtime" : None,
                       "orderby" : "time"}
        # Ensure that the requests ate performed witih the timezone for germany
        self.berlin_time = ZoneInfo("Europe/Berlin")

    def fetch(self) -> ClientResult:
        # Get the data from teh last 24 hrs. Use time in germany
        today = datetime.now(self.berlin_time).replace(microsecond=0)
        yesterday = today - timedelta(days=1)
        self.params["starttime"] = yesterday.isoformat()
        self.params["endtime"] = today.isoformat()
        errors = []
        metadata = []
        response = None
        try:
            response = requests.get(self.url, params=self.params)
            response.raise_for_status()
        except requests.HTTPError as e:
            errors.append({"timestamp":datetime.now().isoformat(),
                           "url":response.url if response else self.url,
                           "error":str(e),
                           "current_symbol":"",
                           "status":response.status_code if response else None})
            df = pd.DataFrame(columns=["timestamp", "magnitude", "scale", "alert", "tsunami", "place", "coordinates"])
        else:
            raw_data = response.json()
            records = self._process(raw_data)
            # store raw data in the data/raw directory
            store(raw_data, "earthquakes")
            df = self._to_dataframe(records)
            metadata.append({"fetched_at":datetime.now().isoformat(),
                             "url":response.url if response else self.url,
                             "status":response.status_code if response else None,
                             "success_count":len(raw_data["features"]),
                             "error_count":len(errors)})
        return ClientResult(data=df, metadata=metadata, errors=errors)

    def _process(self, response:Dict) -> Dict[str,List]:
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
        
    def _to_dataframe(self, records:Dict) -> pd.DataFrame:
        # converts data into geodataframe
        df = pd.DataFrame(records)
        # separte longitude and latitude from the depth
        df["lon"] = df.coordinates.apply(lambda coord: coord[0])
        df["lat"] = df.coordinates.apply(lambda coord: coord[1])
        df["depth"] = df.coordinates.apply(lambda coord: coord[2])
        df = df.drop("coordinates", axis=1)
        # rename columns for better understanding
        df = df.rename(columns={"time":"timestamp", "mag":"magnitude", "magType":"scale"})
        df = df.replace(EarthQuakeClient.MAGNITUDE_TYPE_DESCRIPTION)       
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms") # Timestamps in the responses are in ms
        df["split_on"] = df["timestamp"].dt.strftime("date_%Y_%m_%d")
        return df
    
if __name__ == "__main__":
    # quick tests
    from pathlib import Path

    client = EarthQuakeClient()
    earthquakes = client.fetch()
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "data" / "raw" / "earthquakes" / "earthquake_test.csv"
    if earthquakes.data is not None:
        earthquakes.data.to_csv(output_path, index=False)
    
