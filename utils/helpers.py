import csv
import json
from pathlib import Path
from datetime import datetime
from typing import List

def get_url(api_name:str) -> str|None:
    with Path(__file__).parent.parent.joinpath(r"pipelines/sources.csv").open(newline="") as source:
        reader = csv.DictReader(source, fieldnames=("name","url","access_method","data_type","notes"), delimiter=";")
        for row in reader:
            if row["name"] == api_name:
                return row["url"]
    return None

def load_openweather_cities() -> List[str]:
    with Path(__file__).parent.parent.joinpath(r"config/cities.csv").open(newline="") as source:
        reader = csv.DictReader(source, fieldnames=("city","country"), delimiter=";")
        cities = [row["city"] for row in reader]
    return cities

def store(raw_data:dict, api_name:str) -> None:
    # get the project's root directory
    raw_data_directory = Path(__file__).resolve().parents[1].joinpath(fr"data/raw/{api_name}")
    raw_data_directory.mkdir(parents=True, exist_ok=True)
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = raw_data_directory.joinpath(f"{current_time}_{api_name}.json")
    with file_name.open("w") as output:
        json.dump(raw_data, output, indent=4)

def load_alpha_vantage_symbols() -> List[str]:
    with Path(__file__).parent.parent.joinpath(r"config/alphavantage_symbols.csv").open(newline="") as source:
        reader = csv.DictReader(source, fieldnames=("name","symbol"), delimiter=";")
        symbols = [row["symbol"] for row in reader]
    return symbols