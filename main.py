import calc_Dunkelflaute
import calc_pv
import calc_wind
import find_data
import helper_functions as hpf

if __name__ == "__main__":
    overwrite_existing = False

    find_data.find_nukleus_files()
    nukleus_folders = hpf.load_folder_locations()

    for folder_dict in nukleus_folders:
        cf_wind = calc_wind.cf_wind(nukleus_folders[folder_dict], overwrite_existing)
        cf_pv = calc_pv.calculate_pv(nukleus_folders[folder_dict], overwrite_existing)
        calc_Dunkelflaute.calculate_dunkelflaute(cf_wind, cf_pv, overwrite_existing)
