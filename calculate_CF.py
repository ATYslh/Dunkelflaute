import json

import calc_pv
import calc_wind


def load_folder_locations() -> dict:
    with open('nukleus_files.json', 'r', encoding='utf-8') as file:
        return json.load(file)


def calculate_capacity_factors(overwrite_existing: bool = False):
    nukleus_folders = load_folder_locations()
    for folder_dict in nukleus_folders:
        calc_wind.cf_wind(nukleus_folders[folder_dict], overwrite_existing)
        calc_pv.calculate_pv(nukleus_folders[folder_dict], overwrite_existing)