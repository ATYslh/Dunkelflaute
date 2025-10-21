"""
This module calculates the windspeed and the wind capacity factor for a 5MW turbine.
"""

import hashlib
from multiprocessing import Pool

import helper_functions as hpf
import numpy as np
import xarray as xr

# Wind speed and power curve for 5MW turbine
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


def _power_curve_5(ws: np.ndarray) -> np.ndarray:
    """
    Calculates the power curve for 5MW turbine.
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
    Calculates the wind speed from u and v components.
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
        f"touch {outfile}",
        1,
    )

    hpf.run_shell_command(
        f"cdo -expr,'sfcWind=hypot(ua250m,va250m)' -merge {u_dummy} {v_dummy} {outfile}",
        5,
    )


def calc_wind_capacity_factor(input_file: str, output_file: str) -> None:
    """
    Calculates the wind capacity factor using the 5MW turbine curve.
    """
    with xr.open_dataset(input_file) as ds:
        power = (
            xr.apply_ufunc(
                _power_curve_5,
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
        calculate_wind(u_wind, v_wind, wind_file)

        calc_wind_capacity_factor(
                input_file=wind_file,
                output_file=cf_file,
            )

    except Exception as e:
        raise RuntimeError(
            f"Failed to process wind task with inputs: index={index}, "
            f"u_wind={repr(u_wind)}, v_wind={repr(v_wind)}. Error: {e}"
        ) from e

    return wind_file, cf_file

def cf_wind() -> None:
    """
    Main function for calculating wind speed fields and their capacity factors for 5MW turbine.
    """
    folders = [
        "/work/bb1203/data_NUKLEUS_CMOR/CEU-3/CLMcom-BTU/EC-Earth-Consortium-EC-Earth3-Veg/historical/r1i1p1f1/CLMcom-BTU-ICON-2-6-5-rc/nukleus-x2yn2-v1/1hr/ua250m/v20240201",
        "/work/bb1203/data_NUKLEUS_CMOR/CEU-3/CLMcom-BTU/EC-Earth-Consortium-EC-Earth3-Veg/ssp370-GWL2K/r1i1p1f1/CLMcom-BTU-ICON-2-6-5-rc/nukleus-x2yn2-v1/1hr/ua250m/v20240201",
        "/work/bb1203/data_NUKLEUS_CMOR/CEU-3/CLMcom-BTU/EC-Earth-Consortium-EC-Earth3-Veg/ssp370-GWL3K/r1i1p1f1/CLMcom-BTU-ICON-2-6-5-rc/nukleus-x2yn2-v1/1hr/ua250m/v20240201",
    ]

    for folder in folders:
        scenario = folder.split("/")[7]
        output_filename = f"../Data/250m/wind_{scenario}.nc"
        cf_wind_output = f"../Data/250m/cf_wind_{scenario}.nc"

        hpf.run_shell_command(
            "rm -f /scratch/g/g260190/wind_???.nc /scratch/g/g260190/cf_wind_???.nc", 5
        )

        u_files = hpf.get_sorted_nc_files(folder)
        v_files = hpf.get_sorted_nc_files(folder.replace("ua250m", "va250m"))

        if len(u_files) != len(v_files):
            raise ValueError("ua250m and va250m folders have different file counts")

        params = [(idx, u, v) for idx, (u, v) in enumerate(zip(u_files, v_files))]

        num_procs = hpf.process_input_args()
        with Pool(processes=num_procs) as pool:
            for _, _ in pool.imap_unordered(process_wind_task, params, chunksize=1):
                pass

        hpf.run_shell_command(f"rm -f {output_filename}", 5)
        hpf.run_shell_command(
            f"cdo -s -z zip -cat /scratch/g/g260190/wind_???.nc {output_filename}", 60
        )

        hpf.run_shell_command(f"rm -f {cf_wind_output}", 5)
        hpf.run_shell_command(
            f"cdo -s -z zip -cat /scratch/g/g260190/cf_wind_???.nc {cf_wind_output}", 60
        )

        return None

if __name__ == "__main__":
    cf_wind()