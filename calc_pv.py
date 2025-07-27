"""
This module calculates the PV capacity factor using temperature (tas) and radiation (rsds).
"""

import os
from multiprocessing import Pool

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
        1 + c1 * np.log(g_norm) + c2 * np.log(g_norm) ** 2 + b * delta_t
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


def calculate_capacity_factor_pv(tas: str, rsds: str, output_filename: str) -> None:
    """
    Read tas and rsds, compute CF_PV, apply spatial mask via xarray, and save result.

    Parameters
    ----------
    tas, rsds : str
        Input NetCDF file paths for temperature and surface radiation.
    output_filename : str
        Final NetCDF file path to write.
    """
    # 1. figure out the mask file and the regional slice
    mask_path = hpf.mask_path(rsds)
    i0, i1, j0, j1 = map(int, hpf.get_indexbox(rsds).split(","))
    sel = dict(rlon=slice(i0, i1 + 1), rlat=slice(j0, j1 + 1))

    # 2. open mask, tas, rsds in one context and slice them
    with xr.open_dataset(mask_path) as mask_ds, xr.open_dataset(tas).isel(
        **sel
    ) as ds_tas, xr.open_dataset(rsds).isel(**sel) as ds_rsds:

        mask = mask_ds["MASK"]  # (rlat, rlon)
        tas_c = ds_tas["tas"] - 273.15  # convert to °C
        g = ds_rsds["rsds"].where(ds_rsds["rsds"] >= 1, 1)  # avoid zeros

        # 3. do the numpy-based PV efficiency & capacity computations
        h_rel = relative_efficiency(g.values, tas_c.values)
        pv_cap_np = capacity_factor(h_rel, g.values)

        # 4. wrap back into xarray, reapply mask
        da = xr.DataArray(
            pv_cap_np, coords=tas_c.coords, dims=tas_c.dims, name="CF_PV"
        ).where(mask == 1)

        # preserve metadata
        da.attrs["long_name"] = "PV capacity factor"
        da.attrs["units"] = "1"
        da["lat"].attrs = ds_tas["lat"].attrs
        da["lon"].attrs = ds_tas["lon"].attrs

        # 5. write out
        da.to_dataset().to_netcdf(output_filename)


def _process_pv_task(args):
    index, tas_file, rsds_file = args
    out_file = f"/scratch/g/g260190/pv_{index:03}.nc"
    calculate_capacity_factor_pv(tas_file, rsds_file, out_file)
    return out_file


def calculate_pv_main(folder_dict: dict, overwrite_existing: bool) -> str:
    """
    Loop through sorted tas/rsds files, compute per-file CF_pv, then concatenate.

    Returns
    -------
    Path
        Final concatenated CF_pv NetCDF file.
    """
    # remove any old intermediates
    hpf.run_shell_command("rm -f /scratch/g/g260190/pv_*.nc", 5)

    output_filename = hpf.generate_filename(folder_dict["rsds"], "CF_PV")
    cf_pv_output = os.path.join("CF_PV", output_filename)
    if not overwrite_existing and os.path.exists(cf_pv_output):
        return cf_pv_output

    # collect inputs
    tas_files = hpf.get_sorted_nc_files(folder_dict["tas"])
    rsds_files = hpf.get_sorted_nc_files(folder_dict["rsds"])
    if len(tas_files) != len(rsds_files):
        raise ValueError("tas and rsds folders do not have the same number of files")

    params = [(i, t, r) for i, (t, r) in enumerate(zip(tas_files, rsds_files))]

    # parallel compute per-file CF_pv
    with Pool(processes=hpf.process_input_args()) as pool:
        for _ in pool.imap_unordered(_process_pv_task, params, chunksize=1):
            pass

    # concatenate and clean up
    hpf.run_shell_command(
        f"cdo -s -z zip -cat /scratch/g/g260190/pv_*.nc {cf_pv_output}", 20
    )
    hpf.run_shell_command("rm -f /scratch/g/g260190/pv_*.nc", 5)
    hpf.run_shell_command("rm -f /scratch/g/g260190/tas_*.nc", 5)
    hpf.run_shell_command("rm -f /scratch/g/g260190/rsds_*.nc", 5)

    return cf_pv_output
