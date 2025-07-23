"""
This module calculates the PV capacity factor using temperature (tas) and radiation (rsds).
"""

import os
import time

import numpy as np
import xarray as xr

import helper_functions as hpf


def module_temperature(
    t_air: xr.DataArray,
    g: xr.DataArray,
    t_noct: float = 48,
    t_0: float = 20,
    g_0: float = 800,
) -> xr.DataArray:
    """
    Compute the PV module temperature.

    Parameters
    ----------
    t_air : xr.DataArray
        Air temperature in °C.
    g : xr.DataArray
        Surface downward shortwave radiation (W/m²).
    t_noct : float
        Nominal operating cell temperature (°C). Default 48.
    t_0 : float
        Reference ambient temperature (°C). Default 20.
    g_0 : float
        Reference irradiance (W/m²). Default 800.

    Returns
    -------
    xr.DataArray
        Module temperature (°C).
    """
    return t_air + (t_noct - t_0) * (g / g_0)


def relative_efficiency(
    g: xr.DataArray,
    t_air: xr.DataArray,
    g_stc: float = 1000,
    t_stc: float = 25,
) -> xr.DataArray:
    """
    Compute the PV relative efficiency adjustment factor.

    Uses polynomial fit parameters from the referenced paper.

    Returns
    -------
    xr.DataArray
        Dimensionless efficiency multiplier.
    """
    # Constants
    a = 1.20e-3
    b = -4.60e-3
    c1 = 0.033
    c2 = -0.0092

    # Normalize irradiance and compute module temperature
    g_norm = g / g_stc
    t_mod = module_temperature(t_air, g)
    delta_t = t_mod - t_stc

    # Efficiency model
    eff = (1 + a * delta_t) * (
        1
        + c1 * np.log(g_norm)
        + c2 * np.log(g_norm) ** 2
        + b * delta_t
    )
    return eff


def capacity_factor(
    h_rel: xr.DataArray, g: xr.DataArray, g_stc: float = 1000
) -> xr.DataArray:
    """
    Compute PV capacity factor from relative efficiency and irradiance.

    Returns
    -------
    xr.DataArray
        Capacity factor (0–1).
    """
    return h_rel * (g / g_stc)


def calculate_capacity_factor_pv(tas: str, rsds: str, output_filename: str):
    """
    Read tas and rsds, compute CF_pv, apply spatial mask via CDO, and save result.

    Parameters
    ----------
    tas, rsds : str
        Input NetCDF file paths for temperature and radiation.
    output_filename : str
        Final NetCDF file path to write.
    """
    with xr.open_dataset(tas) as ds_tas, xr.open_dataset(rsds) as ds_rsds:
        # convert to °C
        tas_c = ds_tas["tas"] - 273.15
        g = ds_rsds["rsds"]
        g = xr.where(g < 1, 1, g)

        # compute
        h_rel = relative_efficiency(g.values, tas_c.values)
        pv_cap_np = capacity_factor(h_rel, g.values)

        # wrap into xarray (reuse coords/dims)
        da = xr.DataArray(
            pv_cap_np,
            coords=tas_c.coords,
            dims=tas_c.dims,
            name="CF_pv",
        )
        da.attrs["long_name"] = "PV capacity factor"
        da.attrs["units"] = "1"  # unitless fraction

        # piggy-back lat/lon attrs if you like
        da["lat"].attrs = ds_tas["lat"].attrs
        da["lon"].attrs = ds_tas["lon"].attrs

        da.to_netcdf("/scratch/g/g260190/dummy.nc")
        os.system(
            f"cdo -selindexbox,{hpf.get_indexbox(rsds)} -ifthen "
            f"{hpf.mask_path(rsds)} /scratch/g/g260190/dummy.nc {output_filename}"
        )


def calculate_pv_main(folder_dict: dict, overwrite_existing: bool) -> str:
    """
    Loop through sorted tas/rsds files, compute per-file CF_pv, then concatenate.

    Returns
    -------
    Path
        Final concatenated CF_pv NetCDF file.
    """

    os.system("rm /scratch/g/g260190/pv*.nc")
    output_filename = hpf.generate_filename(folder_dict["rsds"], "CF_PV")
    cf_pv_output = os.path.join('CF_PV', output_filename)
    if not overwrite_existing and os.path.exists(cf_pv_output):
        return cf_pv_output

    tas_folder = folder_dict["tas"]
    rsds_folder = folder_dict["rsds"]

    tas_files = hpf.get_sorted_nc_files(tas_folder)
    rsds_files = hpf.get_sorted_nc_files(rsds_folder)

    if len(tas_files) != len(rsds_files):
        raise ValueError("tas and rsds folders do not have the same number of files")

    for index, (tas, rsds) in enumerate(zip(tas_files, rsds_files)):
        start_time = time.time()
        calculate_capacity_factor_pv(tas, rsds, f"/scratch/g/g260190/pv_{index:03}.nc")
        end_time = time.time()
        print(f"Execution time calculate CF PV: {end_time - start_time:.4f} seconds")

    os.system(f"cdo -z zip -cat /scratch/g/g260190/pv_???.nc {cf_pv_output}")
    os.system("rm /scratch/g/g260190/pv*.nc")
    return cf_pv_output
