import json
import os
import sys

import numpy as np
import xarray as xr


def load_json_file(json_file: str) -> dict:
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


def wind_in_time_period(
    dataset: xr.Dataset, time_info: dict, file_name: str, scenario: str
) -> np.ndarray:
    """Extract wind data for a specific time period."""
    start = f"{time_info[file_name][scenario]['start']}-01-01"
    end = f"{time_info[file_name][scenario]['end']}-12-31"
    return dataset.sel(time=slice(start, end))["sfcWind"].values


def compute_statistics(
    wind: np.ndarray, region_dict: dict, file_name: str, bins: np.ndarray, scenario: str
) -> dict:
    # 90th percentile
    percentile = np.percentile(wind, 95)
    region_dict[file_name]["95th_percentile"] = percentile.item()

    wind_mean = np.mean(wind, keepdims=True)
    region_dict[file_name]["mean"] = wind_mean.item()

    wind_std = np.std(wind, mean=wind_mean)
    region_dict[file_name]["std"] = wind_std.item()

    # Compute histogram
    counts, _ = np.histogram(wind, bins=bins)
    region_dict[file_name][scenario]["counts"] = counts.tolist()

    return region_dict


def write_json_file(region: str, region_dict: dict) -> None:
    with open(f"Wind/{region}.json", "w") as file:
        json.dump(region_dict, file, indent=4)


def calc_statistics() -> None:
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
    bins = np.linspace(0, 30, 101, dtype=np.float64)

    for region in regions:
        region_dict = {"edges": [round(p, 1) for p in bins]}
        for file_name, scenarios in time_info.items():
            print(f"{region} {file_name}", file=sys.stderr)
            region_dict[file_name] = {}
            full_path = os.path.join(
                "/work/bb1203/g260190_heinrich/Dunkelflaute/Data",
                region,
                "Wind",
                file_name,
            )
            for scenario in scenarios:
                with xr.open_dataset(full_path) as df:
                    region_dict[file_name][scenario] = {}
                    wind = wind_in_time_period(df, time_info, file_name, scenario)
                    region_dict = compute_statistics(
                        wind, region_dict, file_name, bins, scenario
                    )
            write_json_file(region, region_dict)


if __name__ == "__main__":
    calc_statistics()
