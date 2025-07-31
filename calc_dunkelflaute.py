"""
This calculates the dunkelflaute.
"""

import os
from multiprocessing import Pool

import numpy as np
import xarray as xr

import helper_functions as hpf


def dunkelflaute_calcs(cf_wind_file, cf_pv_file, out_file):
    with xr.open_dataset(cf_wind_file) as wind, xr.open_dataset(cf_pv_file) as pv:
        dunkelflaute = np.logical_and(wind["CF_Wind"] < 0.2, pv["CF_PV"] < 0.2)
        dunkelflaute = np.where(np.isnan(wind["CF_Wind"]), np.nan, dunkelflaute)

        da = xr.DataArray(
            dunkelflaute,
            coords=wind.coords,
            dims=wind.dims,
            name="Dunkelflaute",
        )
        da.attrs["long_name"] = "Dunkelflaute"
        da.attrs["units"] = "1"  # unitless fraction

        da.to_netcdf(out_file)


def _process_dunkelflaute_task(args):
    index, cf_wind_file, cf_pv_file = args
    out_file = f"/scratch/g/g260190/dunkelflaute_{index:03}.nc"
    dunkelflaute_calcs(cf_wind_file, cf_pv_file, out_file)
    return out_file


def calculate_dunkelflaute(folder_dict: dict, config: dict) -> None:
    """
    This function calculates the Dunkelflauten file. Uses 20% threshold.
    """
    output_filename = hpf.generate_filename(folder_dict["ua100m"], "Dunkelflaute")
    outfile_name = os.path.join("Dunkelflaute", output_filename)
    if not config["Dunkelflaute"]["overwrite"] and os.path.exists(outfile_name):
        return

    cf_wind_files = hpf.get_sorted_nc_files(
        folder_path="/scratch/g/g260190/", substring="cf_wind"
    )
    cf_pv_files = hpf.get_sorted_nc_files(
        folder_path="/scratch/g/g260190/", substring="pv_"
    )
    if len(cf_wind_files) != len(cf_pv_files):
        raise ValueError("tas and rsds folders do not have the same number of files")

    params = [(i, t, r) for i, (t, r) in enumerate(zip(cf_wind_files, cf_pv_files))]

    # parallel compute per-file CF_pv
    with Pool(processes=hpf.process_input_args()) as pool:
        for _ in pool.imap_unordered(_process_dunkelflaute_task, params, chunksize=1):
            pass

    hpf.run_shell_command(
        f"cdo -s -z zip -cat /scratch/g/g260190/dunkelflaute_*.nc {outfile_name}", 60
    )
