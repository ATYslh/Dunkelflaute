import calc_dunkelflaute
import calc_pv
import calc_wind
import find_data

if __name__ == "__main__":
    overwrite_existing = False

    nukleus_folders = find_data.nukleus_folders(
        file_name="nukleus_files.json", search=True
    )

    for folder_dict in nukleus_folders:
        cf_wind = calc_wind.cf_wind(nukleus_folders[folder_dict], overwrite_existing)
        cf_pv = calc_pv.calculate_pv_main(
            nukleus_folders[folder_dict], overwrite_existing
        )
        calc_dunkelflaute.calculate_dunkelflaute(cf_wind, cf_pv, overwrite_existing)
