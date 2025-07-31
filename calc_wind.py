"""
This module calculates the windspeed and the wind capacity factor.
"""

import hashlib
import os
from multiprocessing import Pool

import numpy as np
import xarray as xr

import helper_functions as hpf

_WIND_3_3 = np.array(
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

_POWER_3_3 = (
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

_WIND_5 = np.array(
    [
        4,
        4.25,
        4.5,
        4.75,
        5,
        5.25,
        5.5,
        5.75,
        6,
        6.25,
        6.5,
        6.75,
        7,
        7.25,
        7.5,
        7.75,
        8,
        8.25,
        8.5,
        8.75,
        9,
        9.25,
        9.5,
        9.75,
        10,
        10.25,
        10.5,
        10.75,
        11,
        11.25,
    ],
    dtype=np.float32,
)

_POWER_5 = (
    np.array(
        [
            224,
            269,
            319,
            376,
            438,
            507,
            583,
            666,
            757,
            856,
            963,
            1078,
            1202,
            1336,
            1479,
            1632,
            1795,
            1969,
            2153,
            2349,
            2556,
            2775,
            3006,
            3249,
            3506,
            3775,
            4058,
            4355,
            4666,
            4992,
        ],
        dtype=np.float32,
    )
    / 5000.0
)


def _power_curve_3_3(ws: np.ndarray) -> np.ndarray:
    """
    Calculates the powercurve for all non-nan values.
    """
    nan_mask = np.isnan(ws)
    w = np.where(nan_mask, 0.0, ws)
    out = np.interp(w, _WIND_3_3, _POWER_3_3, left=0.0, right=1.0)
    out = np.where(w < 3.25, 0.0, np.where(w > 11.0, 1.0, out))
    out = np.where(w > 25.0, 0.0, out)
    out = np.where(nan_mask, np.nan, out)
    return out.astype(np.float32)


def _power_curve_5(ws: np.ndarray) -> np.ndarray:
    """
    Calculates the powercurve for all non-nan values.
    """
    nan_mask = np.isnan(ws)
    w = np.where(nan_mask, 0.0, ws)
    out = np.interp(w, _WIND_5, _POWER_5, left=0.0, right=1.0)
    out = np.where(w < 4, 0.0, np.where(w > 11.25, 1.0, out))
    out = np.where(w > 25.0, 0.0, out)
    out = np.where(nan_mask, np.nan, out)
    return out.astype(np.float32)


def calculate_wind(u_wind: str, v_wind: str, outfile: str) -> None:
    """
    Calculates the wind speed.
    """
    u_dummy = f"/scratch/g/g260190/u_{hashlib.md5(outfile.encode()).hexdigest()}.nc"
    v_dummy = f"/scratch/g/g260190/v_{hashlib.md5(outfile.encode()).hexdigest()}.nc"
    indexbox = hpf.get_indexbox(u_wind, "cdo")
    mask_path = hpf.mask_path(u_wind)
    hpf.run_shell_command(
        f"cdo -ifthen {mask_path} -selindexbox,{indexbox} {u_wind} {u_dummy}", 5
    )
    hpf.run_shell_command(
        f"cdo -ifthen {mask_path} -selindexbox,{indexbox} {v_wind} {v_dummy}", 5
    )
    hpf.run_shell_command(
        f"cdo -expr,'sfcWind=hypot(ua100m,va100m)' "
        f"-merge {u_dummy} {v_dummy} {outfile}",
        5,
    )


def calc_wind_capacity_factor(
    input_file: str, output_file: str, use_power_curve_5: bool
):
    """
    Calculates the wind capacity factor.
    """
    power_curve_fn = _power_curve_5 if use_power_curve_5 else _power_curve_3_3
    with xr.open_dataset(input_file) as ds:
        power = (
            xr.apply_ufunc(
                power_curve_fn,
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
    index, u_wind, v_wind, calc_info = args
    calculte_wind_bool, calculte_cf_wind, use_power_curve_5 = calc_info
    wind_file = f"/scratch/g/g260190/wind_{index:03}.nc"
    cf_file = f"/scratch/g/g260190/cf_wind_{index:03}.nc"

    try:
        # Step 1: generate wind field
        if calculte_wind_bool:
            calculate_wind(u_wind, v_wind, wind_file)

        # Step 2: compute capacity factor
        if calculte_cf_wind:
            calc_wind_capacity_factor(
                input_file=wind_file,
                output_file=cf_file,
                use_power_curve_5=use_power_curve_5,
            )

    except Exception as e:
        raise RuntimeError(
            f"Failed to process wind task with inputs: index={index}, "
            f"u_wind={repr(u_wind)}, v_wind={repr(v_wind)}. Error: {e}"
        ) from e

    return wind_file, cf_file


def early_exit(config: dict, cf_wind_output: str) -> bool:
    if (
        not config["Wind"]["overwrite"]
        and not config["CF_Wind"]["overwrite"]
        and os.path.exists(cf_wind_output)
    ):
        return True
    return False


def check_what_to_calc(
    config: dict, wind_cat: str, output_filename: str
) -> tuple[bool, bool]:
    calculte_wind = True
    if config["Wind"]["split"] and os.path.exists(wind_cat):
        calculte_wind = False
        hpf.split_file(wind_cat, "wind_")

    calculte_cf_wind = True
    if config["CF_Wind"]["split"]:
        calculte_cf_wind = False
        hpf.split_file(output_filename, "cf_wind_")
    return calculte_wind, calculte_cf_wind


def cf_wind(folder_dict: dict, config: dict) -> None:
    """
    Main function for calculating wind speed fields and their capacity factors.
    Returns the path to the concatenated CF file.
    """
    # Prepare output filenames and directories
    output_filename = hpf.generate_filename(folder_dict["ua100m"], "CF_Wind")

    cf_wind_output = os.path.join("CF_Wind", output_filename)

    # Clean up any old intermediate files
    hpf.run_shell_command(
        "rm -f /scratch/g/g260190/wind_???.nc /scratch/g/g260190/cf_wind_???.nc", 5
    )

    wind_cat = os.path.join(
        "Wind", hpf.generate_filename(folder_dict["ua100m"], "wind")
    )

    calculte_wind_bool, calculte_cf_wind = check_what_to_calc(
        config, wind_cat, output_filename
    )

    if early_exit(config, wind_cat):
        return None

    # Gather input file lists
    u_files = hpf.get_sorted_nc_files(folder_dict["ua100m"])
    v_files = hpf.get_sorted_nc_files(folder_dict["va100m"])

    if len(u_files) != len(v_files):
        raise ValueError("ua100m and va100m folders have different file counts")

    # Prepare arguments for each worker
    calc_info = (
        calculte_wind_bool,
        calculte_cf_wind,
        config["CF_Wind"]["use_power_curve_5"],
    )
    params = [
        (idx, u, v, calc_info) for idx, (u, v) in enumerate(zip(u_files, v_files))
    ]

    # Launch Pool and wait for all tasks to finish
    num_procs = hpf.process_input_args()
    with Pool(processes=num_procs) as pool:
        for _, _ in pool.imap_unordered(process_wind_task, params, chunksize=1):
            pass

    # Concatenate all wind fields and CF files into final outputs
    if calculate_wind:
        hpf.run_shell_command(
            f"cdo -s -z zip -cat /scratch/g/g260190/wind_???.nc {wind_cat}", 60
        )

    if calculte_cf_wind:
        hpf.run_shell_command(
            f"cdo -s -z zip -cat /scratch/g/g260190/cf_wind_???.nc {cf_wind_output}", 60
        )

    return None
