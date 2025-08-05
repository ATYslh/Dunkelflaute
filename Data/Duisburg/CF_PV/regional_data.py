import numpy as np
import xarray as xr
import helper_functions as hpf
import os


def region_indices(path: str):
    if "CEU-3" in path:
        if "Duisburg" in path:
            return "16,27,153,164"
        if "Germany" in path:
            return "1,209,1,285"
        if "IAWAK-EE" in path:
            return "166,184,142,161"
        if "ISAP" in path:
            return "56,85,47,67"
        if "KARE" in path:
            return "100,131,3,22"
        if "KlimaKonform" in path:
            return "127,151,101,143"
        if "WAKOS" in path:
            return "40,60,216,239"

    if "EUR-11" in path:
        if "Duisburg":
            return "5,7,39,41"
        if "Germany":
            return "1,52,1,70"
        if "IAWAK-EE":
            return "43,46,36,40"
        if "ISAP":
            return "15,21,13,17"
        if "KARE":
            return "26,33,2,6"
        if "KlimaKonform":
            return "33,38,26,35"
        if "WAKOS":
            return "11,15,55,60"


def bounding_box(arr) -> tuple[int, int, int, int] | None:
    # Get the indices of all elements greater than 0
    coords = np.argwhere(arr > 0)

    # If no element is found, return None (or you can handle it as needed)
    if coords.size == 0:
        return None

    # The bounding box is defined by the minimum and maximum indices along each dimension.
    y_min, x_min = coords.min(axis=0)
    y_max, x_max = coords.max(axis=0)
    return y_min, x_min, y_max, x_max


def write_selindexboxes():
    mask_path = "/work/bb1203/g260190_heinrich/Dunkelflaute/Subregion_Masks/CEU-3/"
    CEU3_MASKS = hpf.get_sorted_nc_files(mask_path)
    for mask in CEU3_MASKS:
        with xr.open_dataset(mask) as df:
            y_min, x_min, y_max, x_max = bounding_box(df.MASK.values)
            print(f"if region == '{os.path.basename(mask).split("_")[0]}':")
            print(f"    return '{x_min+1},{x_max+1},{y_min+1},{y_max+1}'")
    print("-" * 30)

    mask_path = "/work/bb1203/g260190_heinrich/Dunkelflaute/Subregion_Masks/EUR-11/"
    EUR11_MASKS = hpf.get_sorted_nc_files(mask_path)
    for mask in EUR11_MASKS:

        with xr.open_dataset(mask) as df:
            y_min, x_min, y_max, x_max = bounding_box(df.MASK.values)
            print(f"if region == '{os.path.basename(mask).split("_")[0]}':")
            print(f"    return '{x_min+1},{x_max+1},{y_min+1},{y_max+1}'")


def crop_masks():
    # Crop the masks first to the same dimension as the Germany mask 
    # and then further down to the bounding box of their data
    os.chdir(f"/work/bb1203/g260190_heinrich/Dunkelflaute/")
    hpf.run_shell_command(
        "cp -r /work/gg0302/g260190/rsds_analysis/Subregion_Masks/ .", 1
    )
    CEU_3_mask_files = hpf.get_sorted_nc_files(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Subregion_Masks/CEU-3"
    )
    EUR_11_mask_files = hpf.get_sorted_nc_files(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Subregion_Masks/EUR-11"
    )
    mask_files = CEU_3_mask_files + EUR_11_mask_files
    dummy_file = "/scratch/g/g260190/dummy.nc"
    for mask in mask_files:
        indexbox = hpf.get_indexbox(mask, "cdo")
        hpf.run_shell_command(
            f"cdo -selindexbox,{region_indices(mask)} -selindexbox,{hpf.get_indexbox(mask,"cdo")} {mask} {dummy_file}",
            1,
        )
        hpf.run_shell_command(f"mv {dummy_file} {mask}", 1)


def create_folders():
    os.chdir(f"/work/bb1203/g260190_heinrich/Dunkelflaute/Data")
    for region in [
        "Duisburg",
        "Germany",
        "IAWAK-EE",
        "ISAP",
        "KARE",
        "KlimaKonform",
        "WAKOS",
    ]:
        hpf.run_shell_command(f"mkdir {region}", 1)
        hpf.run_shell_command(
            f"mkdir {region}/CF_PV {region}/CF_Wind {region}/Dunkelflaute {region}/Wind {region}/CF_Wind/3_3MW {region}/CF_Wind/5MW",
            1,
        )


def get_mask(path: str, region: str):
    if "EUR-11" in path:
        resolution = "EUR-11"
    elif "CEU-3" in path:
        resolution = "CEU-3"
    else:
        raise ValueError("Cannot determine resolution in get_mask()")
    file = next(
        (
            f
            for f in hpf.get_sorted_nc_files(
                os.path.join(
                    "/work/bb1203/g260190_heinrich/Dunkelflaute/Subregion_Masks",
                    resolution,
                )
            )
            if region in f
        ),
        None,
    )
    return os.path.join(
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Subregion_Masks", resolution, file
    )


import datetime


def create_regional_files():
    index=1

    regions = [
        "Duisburg",
        "IAWAK-EE",
        "ISAP",
        "KARE",
        "KlimaKonform",
        "WAKOS",
    ]
    subfolders = ["CF_PV", "CF_Wind/3_3MW", "CF_Wind/5MW", "Dunkelflaute", "Wind"]
    for region in regions:
        for subfolder in subfolders:
            regional_folder = os.path.join(
                "/work/bb1203/g260190_heinrich/Dunkelflaute/Data", region, subfolder
            )
            germany_folder = os.path.join(
                "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/Germany", subfolder
            )
            print(f"[{index}/30]: Working on {regional_folder} {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}")
            for file in hpf.get_sorted_nc_files(germany_folder):
                output_file=os.path.join(regional_folder,os.path.basename(file))
                if os.path.exists(output_file):
                    continue
                mask = get_mask(file, region)
                hpf.run_shell_command(
                    f"cdo -z zip -ifthen {mask} -selindexbox,{region_indices(mask)} {file} {output_file}",
                    60,
                )

                exit()
            index+=1


if __name__ == "__main__":
    # Germany is skipped because it is the basis for what we do
    hpf.create_gitkeep_in_empty_dirs("/work/bb1203/g260190_heinrich/Dunkelflaute/")
    #create_regional_files()
