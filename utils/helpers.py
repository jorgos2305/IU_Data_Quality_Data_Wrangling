import csv
import json
from pathlib import Path
from datetime import datetime

def get_url(api_name:str) -> str|None:
    with Path(__file__).parent.parent.joinpath(r"config/sources.csv").open(newline="") as source:
        reader = csv.DictReader(source, fieldnames=("name","url","type","notes"), delimiter=";")
        for row in reader:
            if row["name"] == api_name:
                return row["url"]
    return None

def store(raw_data:dict, api_name:str) -> None:
    # get the project's root directory
    raw_data_directory = Path(__file__).resolve().parents[1].joinpath(r"data/raw")
    raw_data_directory.mkdir(parents=True, exist_ok=True)
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = raw_data_directory.joinpath(f"{current_time}_{api_name}.json")
    with file_name.open("w") as output:
        json.dump(raw_data, output, indent=4)