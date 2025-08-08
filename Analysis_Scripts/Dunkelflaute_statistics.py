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


def dunkelflaute_in_time_period(
    time_info: dict, file_name: str, scenario: str
) -> np.ndarray:
    """Extract wind data for a specific time period."""
    start = f"{time_info[file_name][scenario]['start']}-01-01"
    end = f"{time_info[file_name][scenario]['end']}-12-31"
    return start, end


def compute_statistics(
    dataset,
    time_info,
    scenario: str,
) -> dict:

    # select time has to be done by cdo too
    start, end = dunkelflaute_in_time_period(time_info, os.path.basename(dataset), scenario)
    output_path=f"{os.path.dirname(dataset)}"
    return None


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
    time_info = hpf.load_json_file("time.json")

    for region in regions:
        for file_name, scenarios in time_info.items():
            print(f"{region} {file_name} at {datetime.datetime.now()}", file=sys.stderr)
            for turbine in ["5MW", "3_3MW"]:
                dataset = os.path.join(
                    "/work/bb1203/g260190_heinrich/Dunkelflaute/Data",
                    region,
                    "Dunkelflaute",
                    turbine,
                    file_name,
                )
                for scenario in scenarios:
                    compute_statistics(
                        dataset,
                        time_info,
                        scenario,
                    )


if __name__ == "__main__":
    calc_statistics()
