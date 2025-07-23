"""
This module calculates the windspeed and the wind capacity factor.
"""

import os
import sys
import time

import numpy as np
import xarray as xr

import helper_functions as hpf

_WIND = np.array(
    [
        3.25,
        3.5,
        3.75,
        4.0,
        4.25,
        4.5,
        4.75,
        5.0,
        5.25,
        5.5,
        5.75,
        6.0,
        6.25,
        6.5,
        6.75,
        7.0,
        7.25,
        7.5,
        7.75,
        8.0,
        8.25,
        8.5,
        8.75,
        9.0,
        9.25,
        9.5,
        9.75,
        10.0,
        10.25,
        10.5,
        10.75,
        11.0,
    ],
    dtype=np.float32,
)

_POWER = (
    np.array(
        [
            138,
            173,
            223,
            286,
            358,
            440,
            529,
            623,
            722,
            827,
            941,
            1069,
            1211,
            1367,
            1535,
            1715,
            1903,
            2096,
            2290,
            2482,
            2666,
            2835,
            2979,
            3088,
            3159,
            3198,
            3219,
            3232,
            3247,
            3265,
            3282,
            3294,
        ],
        dtype=np.float32,
    )
    / 3300.0
)


def _power_curve(ws: np.ndarray) -> np.ndarray:
    """
    Calculates the powercurve for all non-nan values.
    """
    nan_mask = np.isnan(ws)
    w = np.where(nan_mask, 0.0, ws)
    out = np.interp(w, _WIND, _POWER, left=0.0, right=1.0)
    out = np.where(w < 3.25, 0.0, np.where(w > 11.0, 1.0, out))
    out = np.where(nan_mask, np.nan, out)
    return out.astype(np.float32)


def calculate_wind(u_wind: str, v_wind: str, outfile: str) -> None:
    """
    Calculates the wind speed.
    """
    u_dummy = "/scratch/g/g260190/u_dummy.nc"
    v_dummy = "/scratch/g/g260190/v_dummy.nc"

    os.system(
        f"cdo -ifthen {hpf.mask_path(u_wind)} -selindexbox,{hpf.get_indexbox(u_wind)} {u_wind} {u_dummy}"
    )
    os.system(
        f"cdo -ifthen {hpf.mask_path(u_wind)} -selindexbox,{hpf.get_indexbox(u_wind)} {v_wind} {v_dummy}"
    )

    os.system(
        f"cdo -expr,'sfcWind=hypot(ua100m,va100m)' "
        f"-merge {u_dummy} {v_dummy} {outfile}"
    )


def calc_wind_capacity_factor(input_file: str, output_file: str):
    """
    Calculates the wind capacity factor.
    """
    with xr.open_dataset(input_file) as ds:
        power = (
            xr.apply_ufunc(
                _power_curve,
                ds["sfcWind"],
                dask="parallelized",
                output_dtypes=[np.float32],
            )
            .rename("CF_wind")
            .assign_attrs(units="-")
        )

        power.to_netcdf(
            output_file,
            encoding={"CF_wind": {"zlib": True, "complevel": 4}},
        )


def cf_wind(folder_dict: dict, overwrite_existing: bool) -> str:
    """
    Main function for calculating the wind speed and wind capacity factor.
    """
    os.system("rm /scratch/g/g260190/wind*.nc")
    output_filename = hpf.generate_filename(folder_dict["ua100m"], "CF_Wind")
    cf_wind_output = os.path.join(
        "CF_Wind", hpf.generate_filename(folder_dict["ua100m"], "wind")
    )
    if not overwrite_existing and os.path.exists(cf_wind_output):
        return cf_wind_output

    u_folder = folder_dict["ua100m"]
    v_folder = folder_dict["va100m"]

    u_files = hpf.get_sorted_nc_files(u_folder)
    v_files = hpf.get_sorted_nc_files(v_folder)

    if len(u_files) != len(v_files):
        raise ValueError(
            "ua100m and va100m folders do not have the same number of files"
        )

    for index, (u_wind, v_wind) in enumerate(zip(u_files, v_files)):
        start_time = time.time()

        wind_file = f"/scratch/g/g260190/wind_{index:03}.nc"
        calculate_wind(u_wind, v_wind, wind_file)

        end_time = time.time()
        print(
            f"Execution time calculate Wind: {end_time - start_time:.4f} seconds",
            file=sys.stderr,
        )

        start_time = time.time()
        calc_wind_capacity_factor(
            input_file=wind_file,
            output_file=f"/scratch/g/g260190/cf_wind_{index:03}.nc",
        )

        end_time = time.time()
        print(
            f"Execution time calculate CF Wind: {end_time - start_time:.4f} seconds",
            file=sys.stderr,
        )

    os.system(
        f"cdo -z zip -cat /scratch/g/g260190/wind_???.nc "
        f"{os.path.join('Wind', output_filename)}"
    )
    os.system(f"cdo -z zip -cat /scratch/g/g260190/cf_wind_???.nc {cf_wind_output}")
    os.system("rm /scratch/g/g260190/*wind*.nc")
    return cf_wind_output
