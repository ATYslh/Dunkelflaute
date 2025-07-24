"""
This calculates the dunkelflaute.
"""

import os

import numpy as np
import xarray as xr


def calculate_dunkelflaute(wind_filename: str, pv_filename: str, overwrite_existing: bool) -> None:
    """
    This function calculates the Dunkelflauten file. Uses 20% threshold.
    :param wind_filename:
    :param pv_filename:
    :param overwrite_existing:
    :return: None
    """
    output_filename = os.path.basename(pv_filename).replace("CF_PV", "Dunkelflaute")
    outfile_name = os.path.join(
        "Dunkelflaute", output_filename
    )
    if not overwrite_existing and os.path.exists(outfile_name):
        return

    with xr.open_dataset(wind_filename) as wind, xr.open_dataset(
            pv_filename
    ) as pv:
        dunkelflaute = np.logical_and(
            wind["CF_Wind"] < 0.2, pv["CF_PV"] < 0.2
        )
        dunkelflaute = np.where(np.isnan(wind["CF_Wind"]), np.nan, dunkelflaute)

        da = xr.DataArray(
            dunkelflaute,
            coords=wind.coords,
            dims=wind.dims,
            name="Dunkelflaute",
        )
        da.attrs["long_name"] = "Dunkelflaute"
        da.attrs["units"] = "1"  # unitless fraction

        temp_file_name = "/scratch/g/g260190/dummy.nc"
        da.to_netcdf(temp_file_name)
        os.system(f"cdo -z zip -copy {temp_file_name} {outfile_name}")
