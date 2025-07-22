import json

import calc_wind


def generate_filename(folder: str, variable: str) -> str:
    parts = folder.split("/")
    return "_".join([parts[4], parts[6], parts[7], parts[9], variable])


def load_folder_locations() -> dict:
    with open('nukleus_files.json', 'r', encoding='utf-8') as file:
        return json.load(file)


def calculate_cf_pv(folder: str, overwrite_existing: bool) -> None:
    pass


def calculate_capacity_factors(overwrite_existing: bool = False):
    nukleus_folders = load_folder_locations()
    for folder_dict in nukleus_folders:
        calc_wind.cf_wind(folder_dict, overwrite_existing)
        calculate_cf_pv(folder_dict, overwrite_existing)
