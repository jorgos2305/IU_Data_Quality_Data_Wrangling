import requests
import os
import pandas as pd
import geopandas as gpd
import time
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo
from shapely.geometry import Point
from typing import List, Dict

from utils.helpers import get_url, load_openweather_cities, store

load_dotenv("config/.env")

class OpeanWeatherClient:

    def __init__(self) -> None:
        self.url_geocoding = get_url("geocoding")
        self.url_weather = get_url("weather")
        if self.url_geocoding is None or self.url_weather is None:
            # log error
            raise ValueError("URL not found in source.csv")
        self.apikey = os.getenv("OPENWEATHER_API_KEY")
        self.cities = load_openweather_cities()
        self.params_geocoding = {"q": None, "limit" : 1, "appid" : self.apikey}
        self.params_weather = params = {"units":"metric",
                                        "lon" : None,
                                        "lat" : None,
                                        "date" : None,
                                        "appid" : self.apikey}
        # Ensure that the requests are performed with the timezone for germany
        self.berlin_time = ZoneInfo("Europe/Berlin")

    def fetch(self) -> gpd.GeoDataFrame:
        # get the responses with cities data
        city_responses = self._fetch_city_geocoding()
        # process city responses
        df_geolocations = self._process_city_responses(city_responses)        
        # get weather responses for each city
        weather_responses = self._fetch_weather(df_geolocations)
        # process weather responses
        df_weather = self._process_weather_responses(weather_responses)
        return self._process(df_geolocations, df_weather)
    
    def _process(self, df_geolocations:gpd.GeoDataFrame, df_weather:pd.DataFrame) -> gpd.GeoDataFrame:
        df = pd.concat([df_geolocations, df_weather], axis=1)
        df = df.drop(["name", "lon", "lat"], axis=1)
        return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    
    def _fetch_city_geocoding(self) -> List[Dict]:
        response_cities = []
        for city in self.cities:
            self.params_geocoding["q"] = city
            try:
                response = requests.get(self.url_geocoding, params=self.params_geocoding)
                response.raise_for_status()
            except requests.HTTPError:
                # log the error
                pass
            else:
                response_cities.append(response.json())
                time.sleep(1) # There is a request limit for the OpenWeather API
        return response_cities
    
    def _process_city_responses(self, city_responses:List[Dict]) -> gpd.GeoDataFrame:
        city_coordinates = []
        for response_list in city_responses:
            for response in response_list:
                row = {"city" : response["name"],
                       "country" : response["country"],
                       "lon" : response["lon"],
                       "lat" : response["lat"]}
                city_coordinates.append(row)
        df_geolocations = pd.DataFrame(city_coordinates)
        df_geolocations["geometry"] = df_geolocations.apply(lambda row: Point(row["lon"], row["lat"]), axis=1)
        return gpd.GeoDataFrame(df_geolocations, geometry="geometry", crs="EPSG:4326")
    
    def _fetch_weather(self, df_geolocations:gpd.GeoDataFrame) -> List[Dict]:
        now = datetime.now(self.berlin_time).replace(microsecond=0)
        responses_weather = []
        for _, record in df_geolocations.iterrows():
            lon = record["lon"]
            lat = record["lat"]
            self.params_weather["lon"] = lon
            self.params_weather["lat"] = lat
            self.params_weather["date"] = now.isoformat()
            try:
                response = requests.get(self.url_weather, params=self.params_weather)
                response.raise_for_status()
            except requests.HTTPError:
                # log error
                pass
            else:
                responses_weather.append(response.json())
        store(responses_weather, "weather")
        return responses_weather
            
    def _process_weather_responses(self, weather_respones:List[Dict]) -> pd.DataFrame:
        weather = []
        for response in weather_respones:
            row = {"name" : response["name"], # city name - it might not match no automatic geocoding by the API
                   "temperature" : response["main"]["temp"], # Temperature
                   "temperature_max" : response["main"]["temp_max"], # Max temp at the moment
                   "temperature_min" : response["main"]["temp_min"], # Min temp at the moment
                   "feels_like" : response["main"]["feels_like"], # Human perception of the weather
                   "humidity" : response["main"]["humidity"], # in %
                   "wind_speed" : response["wind"]["speed"], # in m/s
                   "wind_direction" : response["wind"]["deg"],
                   "description" : response["weather"][0]["description"],
                   "timestamp" : datetime.fromtimestamp(response["dt"])}
            weather.append(row)
        return pd.DataFrame(weather)

if __name__ == "__main__":
    # quick tests
    from pathlib import Path

    client = OpeanWeatherClient()
    weather = client.fetch()
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "data" / "raw" / "weather" / "weather_test.csv"
    weather.to_csv(output_path, index=False)
    