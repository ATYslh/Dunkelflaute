from multiprocessing import Pool
import datetime
import importlib.util
import os
import sys
from pathlib import Path
import re
import numpy as np
import xarray as xr
import shutil
import hashlib


def is_remote_cluster():
    cluster_env_vars = ["SLURM_JOB_ID", "PBS_JOBID", "KUBERNETES_SERVICE_HOST"]
    return any(var in os.environ for var in cluster_env_vars)


# Dynamically load helper_functions as hpf
if is_remote_cluster():
    module_path = Path(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data_Scripts/helper_functions.py"
    )
    spec = importlib.util.spec_from_file_location("helper_functions", module_path)
    hpf = importlib.util.module_from_spec(spec)
    sys.modules["helper_functions"] = hpf
    spec.loader.exec_module(hpf)
else:
    import Data_Scripts.helper_functions as hpf


def dunkelflaute_in_time_period(
    time_info: dict, file_name: str, scenario: str
) -> tuple[str, str]:
    start = f"{time_info[file_name][scenario]['start']}-01-01"
    end = f"{time_info[file_name][scenario]['end']}-12-31"
    return start, end


def clean_filename(filename: str) -> str:
    filename = re.sub(r"_historical_", "_", filename)
    filename = re.sub(r"_ssp\d{3}-GWL\dK_", "_", filename)
    filename = re.sub(r"_ssp\d{3}_", "_", filename)
    return os.path.splitext(filename)[0]


def _process_one_file(args) -> tuple[str, dict]:
    """
    Worker for a single file_name + all its scenarios.
    Returns (cleaned_filename, subdict) to be merged into region_dict.
    """
    region, turbine, file_name, scenarios, time_info, variable = args
    cleaned = clean_filename(file_name)
    region_subdict: dict[str, dict] = {}

    # Where the raw Data .nc lives on disk
    full_path = os.path.join(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data",
        region,
        variable,
        turbine,
        file_name,
    )

    # Common scratch base
    scratch_base = os.path.join("/scratch/g", "g260190", variable)
    os.makedirs(scratch_base, exist_ok=True)

    for scenario in scenarios:
        region_subdict[scenario] = {}

        # Create & split seasonal files
        temp_hash = hashlib.md5(full_path.encode()).hexdigest()
        tmpdir = os.path.join(scratch_base, temp_hash)
        os.makedirs(tmpdir, exist_ok=True)

        hpf.run_shell_command(f"cdo splitseas {full_path} {tmpdir}/", 60)

        # all seasons + full year
        datasets = [
            full_path,
            os.path.join(tmpdir, "DJF.nc"),
            os.path.join(tmpdir, "MAM.nc"),
            os.path.join(tmpdir, "JJA.nc"),
            os.path.join(tmpdir, "SON.nc"),
        ]

        for ds in datasets:
            name_only = os.path.splitext(os.path.basename(ds))[0]
            season = name_only if len(name_only) == 3 else "Year"

            start, end = dunkelflaute_in_time_period(time_info, file_name, scenario)
            out_dir = os.path.dirname(full_path)
            out_file = f"{cleaned}_{scenario}_{season}.nc"

            timmean_path = os.path.join(out_dir, "timmean", out_file)
            fldmean_path = os.path.join(out_dir, "fldmean", out_file)
            lt02_path = os.path.join(out_dir, "lt02", out_file)

            if not os.path.exists(os.path.dirname(lt02_path)):
                os.makedirs(os.path.dirname(lt02_path), exist_ok=True)

            compute_tim = not os.path.exists(timmean_path)
            compute_fld = not os.path.exists(fldmean_path)
            compute_lt02 = not os.path.exists(lt02_path)

            input_ds = ds
            if (compute_tim or compute_fld) and "EUR-11" in fldmean_path:
                # subset to the right dates first
                subset = os.path.join(
                    tmpdir, f"{hashlib.md5(timmean_path.encode()).hexdigest()}.nc"
                )
                hpf.run_shell_command(f"cdo seldate,{start},{end} {ds} {subset}", 60)
                input_ds = subset

            if compute_tim:
                hpf.run_shell_command(
                    f"cdo -z zip -timmean {input_ds} {timmean_path}", 60
                )

            if compute_fld:
                hpf.run_shell_command(
                    f"cdo -z zip -fldmean {input_ds} {fldmean_path}", 60
                )

            if compute_lt02:
                hpf.run_shell_command(f"cdo -z zip ltc,0.2 {input_ds} {lt02_path}", 60)

            # compute <0.2
            region_subdict[scenario].setdefault(season, {})
            region_subdict[scenario][season]["timmean"] = timmean_path
            region_subdict[scenario][season]["fldmean"] = fldmean_path
            region_subdict[scenario][season]["lt02"] = lt02_path

        # clean up
        shutil.rmtree(tmpdir)

    return cleaned, region_subdict


def calc_statistics(overwrite: bool = False) -> None:
    regions = [
        "Duisburg",
        "Germany",
        "IAWAK-EE",
        "ISAP",
        "KARE",
        "KlimaKonform",
        "WAKOS",
    ]
    variable = "CF_Wind"
    time_info = hpf.load_json_file(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Analysis_Scripts/time.json"
    )

    for turbine in ["5MW", "3_3MW"]:
        for region in regions:
            output_json = f"{variable}/{region}.json"

            if os.path.exists(output_json):
                region_dict = hpf.load_json_file(output_json)
            else:
                region_dict = {}

            # build pool tasks
            tasks = []
            for file_name, scenarios in time_info.items():
                file_name+=f"_{variable}.nc"
                print(
                    f"{region} {file_name} at {datetime.datetime.now()}",
                    file=sys.stderr,
                )
                # skip if already fully processed?
                cleaned = clean_filename(file_name)
                already = (
                    turbine in region_dict
                    and cleaned in region_dict[turbine]
                    and set(region_dict[turbine][cleaned].keys()) >= set(scenarios)
                )
                if overwrite or not already:
                    tasks.append(
                        (region, turbine, file_name, scenarios, time_info, variable)
                    )

            # parallel execution
            if tasks:
                with Pool(processes=hpf.process_input_args()) as pool:
                    for cleaned, sub in pool.map(_process_one_file, tasks):
                        region_dict.setdefault(turbine, {})
                        region_dict[turbine].setdefault(cleaned, {})
                        # merge without dropping existing scenarios
                        for scenario, seasons in sub.items():
                            region_dict[turbine][cleaned].setdefault(scenario, {})
                            region_dict[turbine][cleaned][scenario].update(seasons)

            # write out after all workers finish
            hpf.write_json_file(output_json, region_dict)


if __name__ == "__main__":
    calc_statistics(True)
