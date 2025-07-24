"""
This module calculates the windspeed and the wind capacity factor.
"""

import os
from multiprocessing import Pool

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
    mask_path = hpf.mask_path(u_wind)
    i0, i1, j0, j1 = map(int, hpf.get_indexbox(u_wind).split(","))
    sel = dict(rlon=slice(i0, i1 + 1), rlat=slice(j0, j1 + 1))
    with xr.open_dataset(mask_path) as mask_ds, xr.open_dataset(u_wind).isel(
        **sel
    ) as ds_u, xr.open_dataset(v_wind).isel(**sel) as ds_v:
        mask = mask_ds["MASK"]

        ds_u = xr.open_dataset(u_wind).isel(**sel)
        ds_v = xr.open_dataset(v_wind).isel(**sel)
        ua = ds_u["ua100m"]
        va = ds_v["va100m"]

        sfcWind = xr.DataArray(
            np.hypot(ua, va), coords=ua.coords, dims=ua.dims, name="sfcWind"
        ).where(mask == 1)

    sfcWind.to_dataset().to_netcdf(outfile)


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
            .rename("CF_Wind")
            .assign_attrs(units="-")
        )

        power.to_netcdf(
            output_file,
            encoding={"CF_Wind": {"zlib": True, "complevel": 4}},
        )


def process_wind_task(args):
    index, u_wind, v_wind = args
    wind_file = f"/scratch/g/g260190/wind_{index:03}.nc"
    cf_file = f"/scratch/g/g260190/cf_wind_{index:03}.nc"

    try:
        # Step 1: generate wind field
        calculate_wind(u_wind, v_wind, wind_file)

        # Step 2: compute capacity factor
        calc_wind_capacity_factor(input_file=wind_file, output_file=cf_file)

    except Exception:
        raise

    return wind_file, cf_file


def cf_wind(folder_dict: dict, overwrite_existing: bool) -> str:
    """
    Main function for calculating wind speed fields and their capacity factors.
    Returns the path to the concatenated CF file.
    """
    # Prepare output filenames and directories
    output_filename = hpf.generate_filename(folder_dict["ua100m"], "CF_Wind")

    cf_wind_output = os.path.join("CF_Wind", output_filename)
    if not overwrite_existing and os.path.exists(cf_wind_output):
        return cf_wind_output

    # Clean up any old intermediate files
    os.system("rm -f /scratch/g/g260190/wind_*.nc /scratch/g/g260190/cf_wind_*.nc")

    # Gather input file lists
    u_files = hpf.get_sorted_nc_files(folder_dict["ua100m"])
    v_files = hpf.get_sorted_nc_files(folder_dict["va100m"])
    if len(u_files) != len(v_files):
        raise ValueError("ua100m and va100m folders have different file counts")

    # Prepare arguments for each worker
    params = [(idx, u, v) for idx, (u, v) in enumerate(zip(u_files, v_files))]

    # Launch Pool and wait for all tasks to finish
    num_procs = hpf.process_input_args()
    with Pool(processes=num_procs) as pool:
        for wind_file, cf_file in pool.imap_unordered(
            process_wind_task, params, chunksize=1
        ):
            pass

    # Concatenate all wind fields and CF files into final outputs
    wind_cat = os.path.join(
        "Wind", hpf.generate_filename(folder_dict["ua100m"], "wind")
    )
    os.system(f"cdo -s -z zip -cat /scratch/g/g260190/wind_*.nc {wind_cat}")
    os.system(f"cdo -s -z zip -cat /scratch/g/g260190/cf_wind_*.nc {cf_wind_output}")

    # Clean up intermediate files
    os.system("rm -f /scratch/g/g260190/wind_*.nc /scratch/g/g260190/cf_wind_*.nc")

    return cf_wind_output
