import xarray as xr
import numpy as np
from pathlib import Path
import importlib.util
import os
import json
import sys

# Dynamically load your helper_functions module
module_path = Path(
    "/work/bb1203/g260190_heinrich/Dunkelflaute/Data_Scripts/helper_functions.py"
)
module_name = "helper_functions"

spec = importlib.util.spec_from_file_location(module_name, module_path)
hpf = importlib.util.module_from_spec(spec)
sys.modules[module_name] = hpf
spec.loader.exec_module(hpf)


def load_json_file(json_file: str):
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


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

    time_info = load_json_file("time.json")
    for region in regions:
        region_dict = {}
        region_dict["edges"] = [
            round(p, 1) for p in np.linspace(0, 30, 101, dtype=np.float64)
        ]
        for file_name in time_info.keys():
            print(file_name)
            region_dict[file_name] = {}
            full_path = os.path.join(
                "/work/bb1203/g260190_heinrich/Dunkelflaute/Data",
                region,
                "Wind",
                file_name,
            )
            for scenario in time_info[file_name].keys():
                with xr.open_dataset(full_path) as df:
                    region_dict[file_name][f"{scenario}"] = {}

                    df = df.sel(
                        time=slice(
                            f"{time_info[file_name][scenario]['start']}-01-01",
                            f"{time_info[file_name][scenario]['end']}-12-31",
                        )
                    )
                    wind = df["sfcWind"].values  # Load data into memory

                    # 90th percentile
                    percentile = np.percentile(wind, 90)
                    region_dict[file_name]["90th_percentile"] = percentile.item()

                    # Define histogram bins
                    bins = np.linspace(0, 30, 101, dtype=np.float64)

                    # Compute histogram
                    counts, edges = np.histogram(wind, bins=bins)
                    region_dict[file_name][f"{scenario}"]["counts"] = counts.tolist()

        with open(f"Wind/{region}.json", "w") as file:
            json.dump(region_dict, file, indent=4)


if __name__ == "__main__":
    calc_statistics()
