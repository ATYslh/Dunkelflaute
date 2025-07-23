import os
import json

def generate_filename(folder: str, variable: str) -> str:
    parts = folder.split("/")
    return "_".join([parts[4], parts[6], parts[7], parts[9], variable])


def get_sorted_nc_files(folder_path):
    nc_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".nc") and os.path.isfile(os.path.join(folder_path, f))
    ]
    return sorted(nc_files)


def get_indexbox(folder: str):
    if "EUR-11" in folder:
        return "190,241,185,254"
    if "CEU-3" in folder:
        return "63,271,70,354"
    raise ValueError("could not identify which indexbox to use")


def mask_path(folder: str) -> str:
    if "EUR-11" in folder:
        resolution = "EUR-11"
    elif "CEU-3" in folder:
        resolution = "CEU-3"
    else:
        raise ValueError("could not identify which mask to use")
    return f"/work/gg0302/g260190/rsds_analysis/Subregion_Masks/{resolution}/Germany_mask.nc"

def load_folder_locations() -> dict:
    with open('nukleus_files.json', 'r', encoding='utf-8') as file:
        return json.load(file)
