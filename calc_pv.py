import os

import numpy as np
import xarray as xr

import helper_functions as hpf


def module_temperature(t_air, g, t_noct=48, t_0=20, g_0=800):
    return t_air + (t_noct - t_0) * (g / g_0)


def relative_efficiency(g, t_air, g_stc=1000, t_stc=25):
    # Constants from the paper
    a = 1.20e-3
    b = -4.60e-3
    c1 = 0.033
    c2 = -0.0092
    g_norm = g / g_stc
    t_mod = module_temperature(t_air, g)
    delta_t = t_mod - t_stc
    h_rel = (1 + a * delta_t) * (
            1 + c1 * np.log(g_norm) + c2 * (np.log(g_norm)) ** 2 + b * delta_t
    )
    return h_rel


def calculate_capacity_factor_pv_main(tas: str, rsds: str, output_filename: str):
    with xr.open_dataset(tas) as ds_tas, xr.open_dataset(rsds) as ds_rsds:
        # convert to °C
        tas_c = ds_tas["tas"] - 273.15
        g = ds_rsds["rsds"]
        g = xr.where(g < 1, 1, g)

        # compute
        h_rel = relative_efficiency(g.values, tas_c.values)
        pv_cap_np = calc_capacity_factor(h_rel, g.values)

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
            f"cdo -selindexbox,{hpf.get_indexbox(rsds)} -ifthen {hpf.mask_path(rsds)} /scratch/g/g260190/dummy.nc {output_filename}"
        )


def calc_capacity_factor(h_rel, g, g_stc=1000):
    return h_rel * (g / g_stc)


def calculate_pv(folder_dict: dict, overwrite_existing: bool):
    output_filename = hpf.generate_filename(folder_dict["rsds"], "pv")
    if not overwrite_existing and os.path.exists(output_filename):
        return

    tas_folder = folder_dict["tas"]
    rsds_folder = folder_dict["rsds"]

    tas_files = hpf.get_sorted_nc_files(tas_folder)
    rsds_files = hpf.get_sorted_nc_files(rsds_folder)

    if len(tas_files) != len(rsds_files):
        raise ValueError("tas and rsds folders do not have the same number of files")

    for index, (tas, rsds) in enumerate(zip(tas_files, rsds_files)):
        calculate_capacity_factor_pv_main(tas, rsds, f"/scratch/g/g260190/pv_{index:03}.nc")
