import datetime
import sys

import calc_dunkelflaute
import calc_pv
import calc_wind
import find_data
import helper_functions as hpf


if __name__ == "__main__":
    overwrite_existing = False

    nukleus_folders = find_data.nukleus_folders(
        file_name="nukleus_files.json", search=False
    )

    for index, folder_dict in enumerate(nukleus_folders):
        loop_start_time = datetime.datetime.now()
        print(
            f"[{index+1}/{len(nukleus_folders)}] Start Wind {folder_dict} at {datetime.datetime.now()}",
            file=sys.stderr,
        )
        calc_wind.cf_wind(nukleus_folders[folder_dict], overwrite_existing)

        print(
            f"[{index+1}/{len(nukleus_folders)}] Start PV {folder_dict} at {datetime.datetime.now()}",
            file=sys.stderr,
        )
        calc_pv.calculate_pv_main(nukleus_folders[folder_dict], overwrite_existing)

        print(
            f"[{index+1}/{len(nukleus_folders)}] Start Dunkelflaute {folder_dict} at {datetime.datetime.now()}",
            file=sys.stderr,
        )
        calc_dunkelflaute.calculate_dunkelflaute(
            nukleus_folders[folder_dict], overwrite_existing
        )
        if (datetime.datetime.now() - loop_start_time).seconds > 120:
            hpf.clean_up()
