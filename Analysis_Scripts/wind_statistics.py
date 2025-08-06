import xarray as xr
import numpy as np
import json
import Data_Scripts.helper_functions as hpf


def calc_statistics():
    regions = [
        "Duisburg",
        "Germany",
        "IAWAK-EE",
        "ISAP",
        "KARE",
        "KlimaKonform",
        "WAKOS",
    ]

    for region in regions:
        dict[region]={}
        files=hpf.get_sorted_nc_files(f"Data/{region}/Wind")
        for file in files:
            with xr.open_dataset(file) as df:
                if "EUR-11" in file:
                    if "historical" in file:
                        df = df.sel(time=slice("1961-01-01", "1990-12-31"))
                    else:
                        df = df.sel(time=slice("1961-01-01", "1990-12-31"))
def statistics_to_json():
    pass

if __name__ == "__main__":
    pass
