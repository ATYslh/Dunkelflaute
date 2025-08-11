import datetime
import importlib.util
import os
import sys
from pathlib import Path

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


def radiation_in_time_period(
    dataset: xr.Dataset, time_info: dict, file_name: str, scenario: str
) -> np.ndarray:
    """Extract radiation data for a specific time period."""
    start = f"{time_info[file_name][scenario]['start']}-01-01"
    end = f"{time_info[file_name][scenario]['end']}-12-31"
    return dataset.sel(time=slice(start, end))["rsds"].values


def compute_statistics(
    df,
    time_info,
    region_dict: dict,
    file_name: str,
    bins: np.ndarray,
    scenario: str,
    season: str,
) -> dict:

    region_dict[file_name][scenario][season] = {}

    rsds = radiation_in_time_period(df, time_info, file_name, scenario)

    # 95th percentile
    percentile = np.percentile(rsds, 95)
    region_dict[file_name][scenario][season]["95th_percentile"] = percentile.item()

    percentile = np.percentile(rsds, 10)
    region_dict[file_name]["10th_percentile"] = percentile.item()

    rsds_mean = np.mean(rsds, keepdims=True)
    region_dict[file_name][scenario][season]["mean"] = rsds_mean.item()

    rsds_std = np.std(rsds, mean=rsds_mean)
    region_dict[file_name][scenario][season]["std"] = rsds_std.item()

    # Compute histogram
    counts, _ = np.histogram(rsds, bins=bins)
    region_dict[file_name][scenario][season]["counts"] = counts.tolist()

    fldmean = df["rsds"].mean(dim=["rlat", "rlon"])

    # Group by hour of the day and compute mean
    diurnal_cycle = fldmean.groupby(df["time"].dt.hour).mean(dim="time")
    region_dict[file_name][scenario][season][
        "diurnal_cycle"
    ] = diurnal_cycle.values.tolist()
    return region_dict


def calc_statistics(overwrite=False) -> None:
    regions = [
        "Duisburg",
        "Germany",
        "IAWAK-EE",
        "ISAP",
        "KARE",
        "KlimaKonform",
        "WAKOS",
    ]

    time_info = hpf.load_json_file("time_rsds.json")
    bins = np.linspace(0, 30, 101, dtype=np.float64)

    for region in regions:
        output_json_file = f"rsds/{region}.json"
        region_dict = {"edges": [round(p, 1) for p in bins]}
        if os.path.exists(output_json_file):
            region_dict = hpf.load_json_file(output_json_file)

        for file_name, scenarios in time_info.items():
            print(f"{region} {file_name} at {datetime.datetime.now()}", file=sys.stderr)
            if file_name in region_dict:
                continue
            region_dict[file_name] = {}
            full_path = os.path.join(
                "/work/bb1203/g260190_heinrich/Dunkelflaute/Data",
                region,
                "rsds",
                file_name,
            )

            for scenario in scenarios:
                region_dict[file_name][scenario] = {}

                hpf.run_shell_command(
                    f"cdo splitseas {full_path} /scratch/g/g260190/", 60
                )
                datasets = [
                    full_path,
                    "/scratch/g/g260190/DJF.nc",
                    "/scratch/g/g260190/MAM.nc",
                    "/scratch/g/g260190/JJA.nc",
                    "/scratch/g/g260190/SON.nc",
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
                        )

            hpf.write_json_file(output_json_file, region_dict)


if __name__ == "__main__":
    calc_statistics()
