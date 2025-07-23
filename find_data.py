"""
This module is used to find all the nukleus folders and files
"""

import json
import os


def find_directories(root_dir: str, frequency: str):
    """
    searches the root directoy for a folder that contains the freq_tokens.
    """
    results = []
    freq_tokens = {"1hr", "3hr", "6hr", "day", "mon"}

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # If this directory matches the desired frequency and has both files, record it
        if frequency in dirpath and {"ua100m", "va100m", "rsds", "tas"}.issubset(
            dirnames
        ):
            results.append(dirpath)

        # If we've hit any frequency folder, don't recurse further
        if any(token in dirpath for token in freq_tokens):
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


def load_folder_locations(json_file: str) -> dict:
    """
    Loads the json file which contains all the Nukleus folder
    that meet the required variable at the correct frequency.
    """
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


def nukleus_folders(file_name="nukleus_files.json", search=True):
    """
    Main function to return the nukleus folders. Searches if wanted, otherwise directly loads
    the json file.
    """
    if search:
        find_nukleus_files(file_name)

    return load_folder_locations(json_file=file_name)
