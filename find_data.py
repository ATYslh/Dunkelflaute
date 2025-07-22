import os
import json


def find_directories(root_dir: str, frequency: str):
    results = []
    freq_tokens = {"1hr", "3hr", "6hr", "day", "mon"}

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # If this directory matches the desired frequency and has both files, record it
        if frequency in dirpath and {"ua100m","va100m", "rsds"}.issubset(dirnames):
            results.append(dirpath)

        # If we've hit any frequency folder, don't recurse further
        if any(token in dirpath for token in freq_tokens):
            dirnames.clear()

    return results


def go_to_version_folder(base_dir: str) -> str | None:
    with os.scandir(base_dir) as entries:
        for entry in entries:
            if entry.is_dir():
                return entry.path
        return None


def find_nukleus_files(base_directory: str = "/work/bb1203/data_NUKLEUS_CMOR/") -> None:
    json_file = "nukleus_files.json"
    json_entries = {}
    for directory in find_directories(root_dir=base_directory, frequency="1hr"):
        if "evaluation" in directory:
            continue

        rsds_folder = go_to_version_folder(os.path.join(directory, "rsds"))
        ua100m_folder = go_to_version_folder(os.path.join(directory, "ua100m"))
        va100m_folder = go_to_version_folder(os.path.join(directory, "va100m"))
        if not rsds_folder or not ua100m_folder or not va100m_folder:
            continue

        json_entries[directory] = {
            "rsds": rsds_folder,
            "ua100m": ua100m_folder,
            "va100m": va100m_folder,
        }

    if json_entries:
        with open(json_file, "w") as file:
            json.dump(json_entries, file, indent=4)


if __name__ == "__main__":
    find_nukleus_files()
