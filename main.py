import calculate_CF
import find_data

if __name__ == "__main__":
    find_data.find_nukleus_files()
    calculate_CF.calculate_capacity_factors(overwrite_existing=False)
