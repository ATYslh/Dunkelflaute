import find_data
import calculate_CF

if __name__ == "__main__":
    find_data.find_nukleus_files()
    calculate_CF.calculate_capacity_factors(overwrite_existing=False)
