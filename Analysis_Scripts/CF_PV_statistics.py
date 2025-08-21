from multiprocessing import Pool
import datetime
import importlib.util
import os
import sys
from pathlib import Path
import re
import shutil
import hashlib

import numpy as np
import xarray as xr

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

DATA_BASE = "/work/bb1203/g260190_heinrich/Dunkelflaute/Data"
SCRATCH_ROOT = "/scratch/g/g260190"

def dunkelflaute_in_time_period(time_info: dict, file_name: str, scenario: str) -> tuple[str, str]:
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
    Process a single region/variable file across all its scenarios.
    Returns (cleaned_filename, subdict) to merge into region_dict.
    """
    region, file_name, scenarios, time_info, variable = args
    cleaned = clean_filename(file_name)
    region_subdict: dict[str, dict] = {}

    # Path to raw .nc file
    full_path = os.path.join(DATA_BASE, region, variable, file_name)

    # Prepare scratch directory for splitting seasons
    scratch_base = os.path.join(SCRATCH_ROOT, variable)
    os.makedirs(scratch_base, exist_ok=True)

    for scenario in scenarios:
        region_subdict[scenario] = {}

        temp_hash = hashlib.md5(full_path.encode()).hexdigest()
        tmpdir = os.path.join(scratch_base, temp_hash)
        os.makedirs(tmpdir, exist_ok=True)

        # split into seasons
        hpf.run_shell_command(f"cdo splitseas {full_path} {tmpdir}/", 60)

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

            # ensure output dirs exist
            for path in (timmean_path, fldmean_path, lt02_path):
                os.makedirs(os.path.dirname(path), exist_ok=True)

            compute_tim = not os.path.exists(timmean_path)
            compute_fld = not os.path.exists(fldmean_path)
            compute_lt02 = not os.path.exists(lt02_path)

            subset_input = ds
            if (compute_tim or compute_fld):
                subset = os.path.join(
                    tmpdir,
                    hashlib.md5(ds.encode()).hexdigest() + ".nc"
                )
                hpf.run_shell_command(f"cdo seldate,{start},{end} {ds} {subset}", 60)
                subset_input = subset

            if compute_tim:
                hpf.run_shell_command(
                    f"cdo -z zip -timmean {subset_input} {timmean_path}", 60
                )
            if compute_fld:
                hpf.run_shell_command(
                    f"cdo -z zip -fldmean {subset_input} {fldmean_path}", 60
                )
            if compute_lt02:
                hpf.run_shell_command(
                    f"cdo -z zip ltc,0.2 {subset_input} {lt02_path}", 60
                )

            region_subdict[scenario].setdefault(season, {})
            region_subdict[scenario][season] = {
                "timmean": timmean_path,
                "fldmean": fldmean_path,
                "lt02": lt02_path,
            }

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
    variable = "CF_PV"
    time_info = hpf.load_json_file(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Analysis_Scripts/time_CF_PV.json"
    )

    for region in regions:
        output_json = f"{variable}/{region}.json"
        if os.path.exists(output_json):
            region_dict = hpf.load_json_file(output_json)
        else:
            region_dict = {}

        # build tasks
        tasks = []
        for file_name, scenarios in time_info.items():
            print(f"{region} {file_name} at {datetime.datetime.now()}", file=sys.stderr)
            cleaned = clean_filename(file_name)
            already = cleaned in region_dict and set(region_dict[cleaned].keys()) >= set(scenarios)
            if overwrite or not already:
                tasks.append((region, file_name, scenarios, time_info, variable))

        if tasks:
            processes = hpf.process_input_args()
            with Pool(processes=processes) as pool:
                for cleaned, sub in pool.map(_process_one_file, tasks):
                    region_dict.setdefault(cleaned, {})
                    for scenario, seasons in sub.items():
                        region_dict[cleaned].setdefault(scenario, {})
                        region_dict[cleaned][scenario].update(seasons)

        hpf.write_json_file(output_json, region_dict)

if __name__ == "__main__":
    calc_statistics(True)
