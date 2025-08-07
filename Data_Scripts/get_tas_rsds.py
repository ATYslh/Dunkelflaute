"""
This module gets temperature (tas) and radiation (rsds) data.
It is not optimal because the exact same procedure is done before to calculate CF_PV,
but I am low and time and this is the easiest way without having to refactor big parts of te code in calc_pv.py
"""

import os
from multiprocessing import Pool

import helper_functions as hpf


def format_data_into_pieces(tas: str, rsds: str, index: int):
    mask_path = hpf.mask_path(rsds)
    indexbox = hpf.get_indexbox(rsds, "cdo")
    tas_dummy = (
        f"/scratch/g/g260190/tas_{index:03}.nc"
    )
    rsds_dummy = f"/scratch/g/g260190/rsds_{index:03}.nc"

    hpf.run_shell_command(
        f"cdo -s -ifthen {mask_path} -selindexbox,{indexbox} {tas} {tas_dummy}", 5
    )
    hpf.run_shell_command(
        f"cdo -s -ifthen {mask_path} -selindexbox,{indexbox} {rsds} {rsds_dummy}", 5
    )


def _process_data_gathering_task(args):
    index, tas_file, rsds_file = args
    format_data_into_pieces(tas_file, rsds_file, index)


def get_data(folder_dict: dict, config: dict) -> None:
    # remove any old intermediates
    hpf.run_shell_command("rm -f /scratch/g/g260190/tas_*.nc", 5)
    hpf.run_shell_command("rm -f /scratch/g/g260190/rsds_*.nc", 5)

    output_filename_tas = hpf.generate_filename(folder_dict["tas"], "CF_PV")
    tas_output = os.path.join("Data/tas/Germany", output_filename_tas)

    output_filename_tas = hpf.generate_filename(folder_dict["rsds"], "RSDS")
    rsds_output = os.path.join("Data/rsds/Germany", output_filename_tas)

    if config["RSDS"]["split"]:
        hpf.split_file(rsds_output, "rsds_")

    if config["TAS"]["split"]:
        hpf.split_file(tas_output, "tas_")

    if not config["TAS"]["overwrite"] and os.path.exists(tas_output) and not config["RSDS"]["overwrite"] and os.path.exists(rsds_output):
        return None

    # collect inputs
    tas_files = hpf.get_sorted_nc_files(folder_dict["tas"])
    rsds_files = hpf.get_sorted_nc_files(folder_dict["rsds"])

    if len(tas_files) != len(rsds_files):
        raise ValueError("tas and rsds folders do not have the same number of files")

    params = [(i, t, r) for i, (t, r) in enumerate(zip(tas_files, rsds_files))]

    # parallel compute per-file CF_pv
    with Pool(processes=hpf.process_input_args()) as pool:
        for _ in pool.imap_unordered(_process_data_gathering_task, params, chunksize=1):
            pass

    # concatenate
    hpf.run_shell_command(f"rm -f {rsds_output}", 5)
    hpf.run_shell_command(f"rm -f {tas_output}", 5)
    hpf.run_shell_command(
        f"cdo -s -z zip -cat /scratch/g/g260190/tas_*.nc {tas_output}", 60
    )
    hpf.run_shell_command(
        f"cdo -s -z zip -cat /scratch/g/g260190/rsds_*.nc {rsds_output}", 60
    )
    return None
