import datetime
import importlib.util
import os
import sys
from pathlib import Path
import re
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
    scenario,
    region_dict,
    cleaned_filename,
    turbine,
    season,
    org_filename,
) -> dict:

    filename = os.path.basename(org_filename)
    start, end = dunkelflaute_in_time_period(time_info, filename, scenario)
    output_path = f"{os.path.dirname(org_filename)}"
    output_filename = f"{cleaned_filename}_{scenario}_{season}.nc"
    path_output_timmean = os.path.join(output_path, "timmean", output_filename)
    path_output_fldmean = os.path.join(output_path, "fldmean", output_filename)

    if not os.path.exists(path_output_timmean):
        hpf.run_shell_command(
            f"cdo -z zip -timmean -seldate,{start},{end} {dataset} {path_output_timmean}",
            60,
        )

    if not os.path.exists(path_output_fldmean):
        hpf.run_shell_command(
            f"cdo -z zip -fldmean -seldate,{start},{end} {dataset} {path_output_fldmean}",
            60,
        )

    region_dict[turbine][cleaned_filename][scenario][season] = {}
    region_dict[turbine][cleaned_filename][scenario][season][
        "timmean"
    ] = path_output_timmean
    region_dict[turbine][cleaned_filename][scenario][season][
        "fldmean"
    ] = path_output_fldmean
    return None


def clean_filename(filename):
    # Replace '_historical_' with '_'
    filename = re.sub(r"_historical_", "_", filename)

    # Replace 'sspXXXGWLX_' with '_'
    filename = re.sub(r"_ssp\d{3}-GWL\dK_", "_", filename)

    return os.path.splitext(filename)[0]


def get_turbine_info(turbine: str):
    if turbine == "5MW":
        time_info = hpf.load_json_file(
            "/work/bb1203/g260190_heinrich/Dunkelflaute/Analysis_Scripts/time_df_5MW.json"
        )
    elif turbine == "3_3MW":
        time_info = hpf.load_json_file(
            "/work/bb1203/g260190_heinrich/Dunkelflaute/Analysis_Scripts/time_df_3_3MW.json"
        )
    else:
        raise ValueError("no turbine detected")
    return time_info


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
    variable = "Dunkelflaute"

    for turbine in ["5MW", "3_3MW"]:
        time_info = get_turbine_info(turbine)

        for region in regions:
            output_json_file = f"{variable}/{region}.json"

            # Load an existing file or start fresh
            if os.path.exists(output_json_file):
                region_dict = hpf.load_json_file(output_json_file)
            else:
                region_dict = {}

            for file_name, scenarios in time_info.items():
                print(
                    f"{region} {file_name} at {datetime.datetime.now()}",
                    file=sys.stderr,
                )
                cleaned_filename = clean_filename(file_name)

                # Ensure both filename and turbine levels exist
                if turbine not in region_dict:
                    region_dict[turbine] = {}

                # 2) Ensure the cleaned filename exists under that turbine
                if cleaned_filename not in region_dict[turbine]:
                    region_dict[turbine][cleaned_filename] = {}

                full_path = os.path.join(
                    "/work/bb1203/g260190_heinrich/Dunkelflaute/Data",
                    region,
                    variable,
                    turbine,
                    file_name,
                )

                for scenario in scenarios:
                    # Skip if this scenario already processed under this turbine
                    if scenario in region_dict[turbine][cleaned_filename]:
                        continue

                    region_dict[turbine][cleaned_filename][scenario] = {}

                    scratch_dir = f"/scratch/g/g260190/{variable}"
                    if not os.path.exists(scratch_dir):
                        os.makedirs(scratch_dir)

                    hpf.run_shell_command(
                        f"cdo splitseas {full_path} {scratch_dir}/", 60
                    )

                    datasets = [
                        full_path,
                        f"{scratch_dir}/DJF.nc",
                        f"{scratch_dir}/MAM.nc",
                        f"{scratch_dir}/JJA.nc",
                        f"{scratch_dir}/SON.nc",
                    ]

                    for dataset in datasets:
                        current_filename = os.path.splitext(os.path.basename(dataset))[
                            0
                        ]
                        season = (
                            current_filename if len(current_filename) == 3 else "Year"
                        )
                        compute_statistics(
                            dataset,
                            time_info,
                            scenario,
                            region_dict,
                            cleaned_filename,
                            turbine,
                            season,
                            full_path,
                        )

                hpf.write_json_file(output_json_file, region_dict)


if __name__ == "__main__":
    calc_statistics()

# One file per turbine spec or one that contains both?
