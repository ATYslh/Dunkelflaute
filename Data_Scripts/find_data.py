"""
This module is used to find all the nukleus folders and files
"""

import datetime
import json
import os
import sys
import helper_functions as hpf
import xarray as xr


def find_directories(root_dir: str, frequency: str) -> list[str]:
    """
    Walk root_dir. Whenever we hit a folder whose name is in freq_tokens,
    we stop recursing and dive into its '1hr' subfolder. If within that
    subfolder we find all required variable-dirs, we record its full path.
    """
    results = []
    freq_tokens = {"1hr", "3hr", "6hr", "day", "mon"}
    required_vars = {"ua100m", "va100m", "rsds", "tas"}

    for dirpath, dirnames, _ in os.walk(root_dir):
        # If this directory contains *any* freq token, we’ll handle it now and skip deeper
        hit_tokens = freq_tokens.intersection(dirnames)
        if hit_tokens:
            # Only dive into the 1hr subfolder
            if frequency in hit_tokens:
                freq_dir = os.path.join(dirpath, frequency)

                # list only the immediate subdirectories of root/.../1hr
                try:
                    subdirs = {
                        name
                        for name in os.listdir(freq_dir)
                        if os.path.isdir(os.path.join(freq_dir, name))
                    }
                except FileNotFoundError:
                    subdirs = set()

                # if it matches the set we need, record it (and only if it’s our requested frequency)
                if frequency == "1hr" and required_vars.issubset(subdirs):
                    results.append(freq_dir)

            # prevent os.walk from descending into any of these freq-token folders
            dirnames.clear()

    return results


def go_to_version_folder(base_dir: str) -> str | None:
    """
    Goes from the variable folder one step further down into the version folder
    """
    with os.scandir(base_dir) as entries:
        for entry in entries:
            if entry.is_dir():
                return entry.path
        return None


def find_nukleus_files(
    json_file: str, base_directory: str = "/work/bb1203/data_NUKLEUS_CMOR/"
) -> None:
    """
    Searches for folders that contain the four required variables with the correct frequency
    """
    json_entries = {}
    for directory in find_directories(root_dir=base_directory, frequency="1hr"):
        if "evaluation" in directory:
            continue

        rsds_folder = go_to_version_folder(os.path.join(directory, "rsds"))
        tas_folder = go_to_version_folder(os.path.join(directory, "tas"))
        ua100m_folder = go_to_version_folder(os.path.join(directory, "ua100m"))
        va100m_folder = go_to_version_folder(os.path.join(directory, "va100m"))
        if not rsds_folder or not ua100m_folder or not va100m_folder:
            continue

        json_entries[directory] = {
            "rsds": rsds_folder,
            "tas": tas_folder,
            "ua100m": ua100m_folder,
            "va100m": va100m_folder,
        }

    if json_entries:
        with open(json_file, "w") as file:
            json.dump(json_entries, file, indent=4)


def load_json_file(json_file: str) -> dict:
    """
    Loads the json file which contains all the Nukleus folder
    that meet the required variable at the correct frequency.
    """
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


def nukleus_folders(file_name="nukleus_files.json", search=True) -> dict:
    """
    Main function to return the nukleus folders. Searches if wanted, otherwise directly loads
    the json file.
    """
    if search:
        find_nukleus_files(file_name)

    return load_json_file(json_file=file_name)


def count_timesteps_in_all_files():
    nukleus_folders = load_json_file("nukleus_files.json")
    time_set = set()

    for index, folder_dict in enumerate(nukleus_folders):
        print(
            f"[{index+1}/{len(nukleus_folders)}] Start {folder_dict} at {datetime.datetime.now()}",
            file=sys.stderr,
        )
        for folder in nukleus_folders[folder_dict]:
            files = hpf.get_sorted_nc_files(nukleus_folders[folder_dict][folder])
            for file in files:
                with xr.open_dataset(file) as df:
                    time_set.add(len(df.time))

    print(time_set)
