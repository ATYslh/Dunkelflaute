"""
Helper functions.
"""

import datetime
import os
import sys

import helper_functions as hpf


def generate_filename(folder: str, variable: str) -> str:
    """
    Generates the filename of the final output.
    """
    parts = folder.split("/")
    return "_".join([parts[4], parts[6], parts[7], parts[9], variable])


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
    """
    if "EUR-11" in folder:
        return "190,241,185,254"
    if "CEU-3" in folder:
        return "63,271,70,354"
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
    hpf.load_folder_locations("nukleus_files.json")
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
