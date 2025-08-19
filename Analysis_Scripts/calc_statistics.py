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
    return dataset.sel(time=slice(start, end))[variable].values


def compute_statistics(
    df,
    time_info,
    region_dict: dict,
    file_name: str,
    bins: np.ndarray,
    scenario: str,
    season: str,
    variable: str,
    cleaned_filename: str,
) -> dict:

    region_dict[cleaned_filename][scenario][season] = {}

    data = data_in_time_period(df, time_info, file_name, scenario, variable)

    # 95th percentile
    percentile = np.nanpercentile(data, 95)
    region_dict[cleaned_filename][scenario][season][
        "95th_percentile"
    ] = percentile.item()

    percentile = np.nanpercentile(data, 10)
    region_dict[cleaned_filename]["10th_percentile"] = percentile.item()

    data_mean = np.nanmean(data, keepdims=True)
    region_dict[cleaned_filename][scenario][season]["mean"] = data_mean.item()

    data_std = np.nanstd(data, mean=data_mean)
    region_dict[cleaned_filename][scenario][season]["std"] = data_std.item()

    # Compute histogram
    counts, _ = np.histogram(data, bins=bins)
    region_dict[cleaned_filename][scenario][season]["counts"] = counts.tolist()

    fldmean = df[variable].mean(dim=["rlat", "rlon"], skipna=True)

    # Group by hour of the day and compute mean
    diurnal_cycle = fldmean.groupby(df["time"].dt.hour).mean(dim="time", skipna=True)
    region_dict[cleaned_filename][scenario][season][
        "diurnal_cycle"
    ] = diurnal_cycle.values.tolist()
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

                hpf.run_shell_command(
                    f"cdo splitseas {full_path} /scratch/g/g260190/{variable}/", 60
                )

                datasets = [
                    full_path,
                    f"/scratch/g/g260190/{variable}/DJF.nc",
                    f"/scratch/g/g260190/{variable}/MAM.nc",
                    f"/scratch/g/g260190/{variable}/JJA.nc",
                    f"/scratch/g/g260190/{variable}/SON.nc",
                ]

                for dataset in datasets:
                    current_filename = os.path.splitext(os.path.basename(dataset))[0]

                    if len(current_filename) == 3:
                        season = current_filename
                    else:
                        season = "Year"
                    with xr.open_dataset(dataset) as df:
                        df.attrs["source_file"] = dataset
                        region_dict = compute_statistics(
                            df,
                            time_info,
                            region_dict,
                            file_name,
                            bins,
                            scenario,
                            season,
                            variable,
                            cleaned_filename,
                        )

            hpf.write_json_file(output_json_file, region_dict)


if __name__ == "__main__":
    calculate_statistics(process_input_args())
