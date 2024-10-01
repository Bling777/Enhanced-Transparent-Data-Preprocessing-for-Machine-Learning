import json
from os.path import abspath, dirname, join

import pandas as pd

from capstone14 import DataProfile

if __name__ == "__main__":
    current_dir = dirname(abspath((__file__)))
    data = pd.read_csv(
        join(current_dir, 'datasets', 'employees_v1.csv'), 
        parse_dates=["entry_date"], 
        date_format='%d.%m.%Y'
    )
    data_profile = DataProfile(dataset=data)
    with open("data_profile.json", "w") as outfile:
        json.dump(data_profile.as_dict(), outfile, indent=4)
