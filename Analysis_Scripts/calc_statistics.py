import datetime
import importlib.util
import os
import re
import sys
from pathlib import Path
import argparse

import numpy as np
import xarray as xr


def is_remote_cluster():
    cluster_env_vars = ["SLURM_JOB_ID", "PBS_JOBID", "KUBERNETES_SERVICE_HOST"]
    return any(var in os.environ for var in cluster_env_vars)


# Dynamically load your helper_functions module
if is_remote_cluster():
    module_path = Path(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data_Scripts/helper_functions.py"
    )
    module_name = "helper_functions"

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    hpf = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = hpf
    spec.loader.exec_module(hpf)

else:
    import Data_Scripts.helper_functions as hpf


def data_in_time_period(
    dataset: xr.Dataset, time_info: dict, file_name: str, scenario: str, variable: str
) -> np.ndarray:
    """Extract data for a specific time period."""
    start = f"{time_info[file_name][scenario]['start']}-01-01"
    end = f"{time_info[file_name][scenario]['end']}-12-31"
    return dataset.sel(time=slice(start, end))


def compute_statistics(
    df,
    time_info,
    region_dict: dict,
    file_name: str,
    bins: np.ndarray,
    scenario: str,
    variable: str,
    cleaned_filename: str,
) -> dict:

    # initial temporal clipping
    data_df = data_in_time_period(df, time_info, file_name, scenario, variable)

    seasons = ["Year", "DJF", "MAM", "JJA", "SON"]
    seasons_array = data_df.time.dt.season
    data = data_df[variable].values  # full-year 1D array

    for season in seasons:
        # 1) isolate the correct slice
        if season == "Year":
            season_df = data_df
            season_data = data
        else:
            # for xarray: drop all times not in this season
            mask = seasons_array == season
            season_df = data_df.where(mask, drop=True)
            season_data = season_df[variable].values

        # ensure nested dicts exist
        region_dict.setdefault(cleaned_filename, {}).setdefault(scenario, {})[
            season
        ] = {}

        sect = region_dict[cleaned_filename][scenario][season]

        if variable == "sfcWind":
            valid = ~np.isnan(season_data)
            under_cut_in = np.sum(season_data < 3) / np.sum(valid)
            above_cut_out = np.sum(season_data > 25) / np.sum(valid)
            sect["under_cut_in"] = under_cut_in.item()
            sect["above_cut_out"] = above_cut_out.item()
            sect["nominal"] = 1 - under_cut_in.item() - above_cut_out.item()

        # percentiles, mean, std, histogram on the 1D array
        sect["95th_percentile"] = np.nanpercentile(season_data, 95).item()
        sect["10th_percentile"] = np.nanpercentile(season_data, 10).item()
        data_mean = np.nanmean(season_data, keepdims=True) 
        region_dict[cleaned_filename][scenario][season]["mean"] = data_mean.item() 
        data_std = np.nanstd(season_data, mean=data_mean) 
        region_dict[cleaned_filename][scenario][season]["std"] = data_std.item()

        counts, _ = np.histogram(season_data, bins=bins)
        sect["counts"] = counts.tolist()

        # now use the *seasonal* DataFrame for the diurnal cycle
        fldmean = season_df[variable].mean(dim=["rlat", "rlon"], skipna=True)
        diurnal_cycle = (
            fldmean.groupby("time.hour").mean(dim="time", skipna=True).values.tolist()
        )
        sect["diurnal_cycle"] = diurnal_cycle

    return region_dict


def clean_filename(filename: str) -> str:
    filename = re.sub(r"_historical_", "_", filename)
    filename = re.sub(r"_ssp\d{3}-GWL\dK_", "_", filename)
    filename = re.sub(r"_ssp\d{3}_", "_", filename)
    return os.path.splitext(filename)[0]


def process_input_args() -> str:
    parser = argparse.ArgumentParser(description="Input variable for processing.")
    parser.add_argument(
        "-v",
        "--variable",
        type=str,
        default="sfcWind",
        help="Pick a variable to process",
    )

    args = parser.parse_args()

    return args.variable


def calculate_statistics(variable: str) -> None:
    regions = [
        "Duisburg",
        "Germany",
        "IAWAK-EE",
        "ISAP",
        "KARE",
        "KlimaKonform",
        "WAKOS",
    ]
    time_info = hpf.load_json_file(f"time_{variable}.json")
    bins = np.linspace(0, 30, 101, dtype=np.float64)

    for region in regions:
        output_json_file = f"{variable}/{region}.json"
        region_dict = {"edges": [round(p, 1) for p in bins]}
        if os.path.exists(output_json_file):
            region_dict = hpf.load_json_file(output_json_file)

        for file_name, scenarios in time_info.items():
            print(f"{region} {file_name} at {datetime.datetime.now()}", file=sys.stderr)
            cleaned_filename = clean_filename(file_name)
            if cleaned_filename not in region_dict:
                region_dict[cleaned_filename] = {}
            full_path = os.path.join(
                "/work/bb1203/g260190_heinrich/Dunkelflaute/Data",
                region,
                variable,
                file_name,
            )

            for scenario in scenarios:
                if scenario in region_dict.get(cleaned_filename, {}):
                    continue

                region_dict[cleaned_filename][scenario] = {}
                if not os.path.exists(f"/scratch/g/g260190/{variable}"):
                    os.makedirs(f"/scratch/g/g260190/{variable}")

                current_filename = os.path.splitext(os.path.basename(full_path))[0]

                with xr.open_dataset(full_path) as df:
                    df.attrs["source_file"] = current_filename
                    region_dict = compute_statistics(
                        df,
                        time_info,
                        region_dict,
                        file_name,
                        bins,
                        scenario,
                        variable,
                        cleaned_filename,
                    )

            hpf.write_json_file(output_json_file, region_dict)


if __name__ == "__main__":
    calculate_statistics(process_input_args())
