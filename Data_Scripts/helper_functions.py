"""
Helper functions.
"""

import argparse
import os
import subprocess
import sys

import xarray as xr
import yaml

def generate_filename(folder: str, variable: str) -> str:
    """
    Generates the filename of the final output.
    """
    parts = folder.split("/")
    return "_".join([parts[4], parts[6], parts[7], parts[9], variable]) + ".nc"


def get_sorted_nc_files(folder_path: str, substring=None):
    """
    Returns a sorted list of all nc_files in the folder_path.
    """
    nc_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".nc")
        and os.path.isfile(os.path.join(folder_path, f))
        and (substring is None or substring in f)
    ]
    return sorted(nc_files)


def get_indexbox(folder: str, process: str = "xarray") -> str:
    """
    Returns the indices which are used to crop the field to only Germany.
    These are for the xarray version. If you use cdo you need to add +1 to each.
    """
    if "EUR-11" in folder:
        if process == "xarray":
            return "189,240,184,253"
        if process == "cdo":
            return "190,241,185,254"
    if "CEU-3" in folder:
        if process == "xarray":
            return "62,270,69,353"
        if process == "cdo":
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



def process_input_args() -> int:
    parser = argparse.ArgumentParser(
        description="Process input arguments for CMOR check tool."
    )

    parser.add_argument("-c", "--cpu", type=int, default=1, help="SLURM CPUS PER TASK")
    args = parser.parse_args()

    return args.cpu


def run_shell_command(command: str, time_minutes: int) -> None:
    try:
        subprocess.run(command, timeout=60 * time_minutes, shell=True)
    except subprocess.TimeoutExpired:
        print(f"The following command timed out: {command}", file=sys.stderr)


def clean_up():
    run_shell_command(
        "rm -f /scratch/g/g260190/wind_*.nc /scratch/g/g260190/cf_wind_*.nc", 5
    )
    run_shell_command("rm -f /scratch/g/g260190/pv_*.nc", 5)
    run_shell_command("rm -f /scratch/g/g260190/tas_*.nc", 5)
    run_shell_command("rm -f /scratch/g/g260190/rsds_*.nc", 5)
    run_shell_command("rm -f /scratch/g/g260190/dummy.nc", 5)
    run_shell_command("rm -f /scratch/g/g260190/u_*.nc", 5)
    run_shell_command("rm -f /scratch/g/g260190/v_*.nc", 5)


def read_config_file(path: str) -> dict:
    with open(path, "r") as file:
        config = yaml.safe_load(file)
        return config


def split_file(path: str, prefix: str) -> None:
    scratch = "/scratch/g/g260190/"
    run_shell_command(f"cdo splityear {path} {scratch}{prefix}", 20)
    # rename the split files
    nc_files = get_sorted_nc_files(scratch, prefix)
    print(f"Got the command: cdo splityear {path} {scratch}{prefix}", file=sys.stderr)
    print(f"Found files: {nc_files}", file=sys.stderr)
    for i, filename in enumerate(nc_files):
        new_name = f"{scratch}{prefix}{i:03d}.nc"
        os.rename(filename, new_name)


def create_gitkeep_in_empty_dirs(root_dir):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Skip hidden directories
        if any(part.startswith(".") for part in dirpath.split(os.sep)):
            continue

        # Filter out hidden subdirectories from dirnames to prevent descending into them
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        # Check if the directory is empty (no files and no subdirectories)
        if not dirnames and not filenames:
            gitkeep_path = os.path.join(dirpath, ".gitkeep")
            if not os.path.exists(gitkeep_path):
                with open(gitkeep_path, "w"):
                    pass  # Create an empty .gitkeep file
                print(f"Created .gitkeep in: {dirpath}")
