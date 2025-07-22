import os


def get_sorted_nc_files(folder_path):
    nc_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".nc") and os.path.isfile(os.path.join(folder_path, f))
    ]
    return sorted(nc_files)
