import os

import yaml


def find_directories(root_dir:str, frequency:str):
    results = []
    for dirpath, _, _ in os.walk(root_dir):
        if frequency in dirpath:
            if {"sfcWind", "rsds"}.issubset(os.listdir(dirpath)):
                results.append(dirpath)
    return results

def go_to_version_folder(base_dir:str):
    with os.scandir(base_dir) as entries:
        for entry in entries:
            if entry.is_dir():
                next_dir = entry.path
                return next_dir
        return None

def find_nukleus_files(base_directory = "/work/bb1203/data_NUKLEUS_CMOR/"):
    yaml_file = "nukleus_files.yaml"

    if os.path.exists(yaml_file):
        os.remove(yaml_file)

    for directory in find_directories(root_dir=base_directory, frequency="1hr"):
        if "evaluation" in directory:
            continue

        rsds_folder=go_to_version_folder(os.path.join(directory, 'rsds'))
        sfcWind_folder=go_to_version_folder(os.path.join(directory, 'sfcWind'))
        if not rsds_folder or not sfcWind_folder:
            continue

        new_data = {
            f"{directory}": {
                "rsds": f"{rsds_folder}",
                "sfcWind": f"{sfcWind_folder}",
            }
        }

        # Load existing data if the file exists
        if os.path.exists(yaml_file):
            with open(yaml_file, "r") as file:
                existing_data = yaml.safe_load(file) or {}  # Handle empty file
        else:
            existing_data = {}

        # Merge the new data with existing data
        existing_data.update(new_data)

        # Write the updated data back to the YAML file
        with open(yaml_file, "w") as file:
            yaml.dump(existing_data, file, default_flow_style=False, sort_keys=True)

if __name__ == "__main__":
    find_nukleus_files()
