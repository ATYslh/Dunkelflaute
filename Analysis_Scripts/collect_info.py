import importlib.util
import json
import re
import sys
from pathlib import Path

import pandas as pd
import xarray as xr

# Dynamically load your helper_functions module
module_path = Path(
    "/work/bb1203/g260190_heinrich/Dunkelflaute/Data_Scripts/helper_functions.py"
)
module_name = "helper_functions"

spec = importlib.util.spec_from_file_location(module_name, module_path)
hpf = importlib.util.module_from_spec(spec)
sys.modules[module_name] = hpf
spec.loader.exec_module(hpf)


def extract_time_range(nc_path: Path) -> dict[str, int]:
    """
    Open a NetCDF file, read the first and last time values,
    and return their calendar years.
    """
    with xr.open_dataset(nc_path) as ds:
        times = ds["time"].values

    start_year = pd.to_datetime(times[0]).year
    end_year = pd.to_datetime(times[-1]).year

    return {"start": int(start_year), "end": int(end_year)}


def extract_scenario_key(file_name: str) -> str | None:
    """
    Return 'historical' if present, otherwise the first GWLxxxK tag.
    """
    lower = file_name.lower()
    if "historical" in lower:
        return "historical"

    match = re.search(r"GWL\d+K", file_name)
    return match.group(0) if match else None

def same_except_one_digit(s1: str, s2: str) -> bool:
    # quick length check
    if len(s1) != len(s2):
        return False

    # collect all positions where they differ
    diffs = [(c1, c2) for c1, c2 in zip(s1, s2) if c1 != c2]

    # must be exactly one mismatch, and both chars there are digits
    return (
        len(diffs) == 1
        and diffs[0][0].isdigit()
        and diffs[0][1].isdigit()
    )

def find_matching_eur11(ceu3_path: Path, eur11_paths: list[Path]) -> Path | None:
    """
    Given a CEU-3 file path, replace 'CEU-3' with 'EUR-11', strip GWL tags,
    and look for a matching name in eur11_paths.
    """
    candidate = ceu3_path.name.replace("CEU-3", "EUR-11")
    candidate_clean = re.sub(r"-GWL\d+K", "", candidate)

    for p in eur11_paths:
        if candidate_clean in p.name:
            return p
    
    #Because Hereon EUR-11 uses clm3, while CEU-3 uses clm2
    for p in eur11_paths:
        if same_except_one_digit(str(p.name),candidate_clean):
            return p
    return None


def gather_time_information(data_dir: str, output_file: str = "time.json") -> None:
    """
    Walk through all NetCDF files returned by get_sorted_nc_files,
    extract start/end years for CEU-3 files (skipping EUR-11),
    pair them with their EUR-11 counterparts, and write out JSON.
    """
    data_dir = Path(data_dir)
    all_paths=[Path(p) for p in hpf.get_sorted_nc_files(data_dir)]

    ceu3_files = [p for p in all_paths if "CEU-3" in p.name and "EUR-11" not in p.name]
    eur11_files = [p for p in all_paths if "EUR-11" in p.name]

    info: dict[str, dict[str, dict[str, int]]] = {}

    for ceu3 in ceu3_files:
        scenario = extract_scenario_key(ceu3.name)
        if not scenario:
            print(f"Skipping {ceu3.name!r}: no scenario tag found")
            continue

        time_range = extract_time_range(ceu3)
        info.setdefault(ceu3.name, {})[scenario] = time_range

        eur11 = find_matching_eur11(ceu3, eur11_files)
        if eur11:
            info.setdefault(eur11.name, {})[scenario] = time_range
        else:
            print(f"No EUR-11 counterpart for {ceu3.name}")

    with open(output_file, "w") as fout:
        json.dump(info, fout, indent=4)


if __name__ == "__main__":
    gather_time_information(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/Duisburg/Wind", "time.json"
    )
