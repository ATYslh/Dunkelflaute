"""
Helper functions.
"""

import datetime
import os
import sys

import helper_functions as hpf
import xarray as xr
import argparse


def generate_filename(folder: str, variable: str) -> str:
    """
    Generates the filename of the final output.
    """
    parts = folder.split("/")
    return "_".join([parts[4], parts[6], parts[7], parts[9], variable]) + ".nc"


def get_sorted_nc_files(folder_path):
    """
    Returns a sorted list of all nc_files in the folder_path.
    """
    nc_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".nc") and os.path.isfile(os.path.join(folder_path, f))
    ]
    return sorted(nc_files)


def get_indexbox(folder: str):
    """
    Returns the indices which are used to crop the field to only Germany.
    These are for the xarray version. If you use cdo you need to add +1 to each.
    """
    if "EUR-11" in folder:
        return "189,240,184,253"
    if "CEU-3" in folder:
        return "62,270,69,353"
    raise ValueError("could not identify which indexbox to use")


def mask_path(folder: str) -> str:
    """
    Returns the path to the mask file.
    """
    if "EUR-11" in folder:
        resolution = "EUR-11"
    elif "CEU-3" in folder:
        resolution = "CEU-3"
    else:
        raise ValueError("could not identify which mask to use")
    return f"{resolution}_mask.nc"


def count_timesteps_in_all_files():
    nukleus_folders = hpf.load_folder_locations("nukleus_files.json")
    time_set = set()

    for index, folder_dict in enumerate(nukleus_folders):
        print(
            f"[{index+1}/{len(nukleus_folders)}] Start {folder_dict} at {datetime.datetime.now()}",
            file=sys.stderr,
        )
        for folder in nukleus_folders[folder_dict]:
            files = get_sorted_nc_files(nukleus_folders[folder_dict][folder])
            for file in files:
                with xr.open_dataset(file) as df:
                    time_set.add(len(df.time))

    print(time_set)


def process_input_args() -> int:
    parser = argparse.ArgumentParser(
        description="Process input arguments for CMOR check tool."
    )

    parser.add_argument("-c", "--cpu", type=int, default=1, help="SLURM CPUS PER TASK")
    args = parser.parse_args()

    return args.cpu
