import json
import os
import subprocess
import sys

import numpy as np
import xarray as xr


def run_shell_command(command: str, time_minutes: int) -> None:
    try:
        subprocess.run(command, timeout=60 * time_minutes, shell=True)
    except subprocess.TimeoutExpired:
        print(f"The following command timed out: {command}", file=sys.stderr)


def write_json_file(filename: str, content: dict) -> None:
    with open(filename, "w") as file:
        json.dump(content, file, indent=4)


def main():
    timmean_path = "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/timmean"
    temp_folder = "/scratch/g/g260190/"
    bins = np.linspace(0, 30, 101, dtype=np.float64)
    seasons = ["DJF", "MAM", "JJA", "SON"]
    scenarios=["historical","ssp370-GWL2K","ssp370-GWL3K"]
    files = [
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/wind_historical.nc",
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/wind_ssp370-GWL2K.nc",
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/wind_ssp370-GWL3K.nc",
    ]

    for scenario, file in zip(scenarios,files):
        stats = {}
        # split into seasons for frequency analysis
        run_shell_command(f"cdo splitseas {file} {temp_folder}", 60)

        for season in seasons:
            season_file = os.path.join(temp_folder, f"{season}.nc")
            with xr.open_dataset(season_file) as df:
                counts, _ = np.histogram(df["sfcWind"].values, bins=bins)
                stats[season] = counts.tolist()

        write_json_file(
            f"/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/frequency/{scenario}.json",
            stats,
        )
        # calc timmean for entire year
        file_name = os.path.basename(file)
        timmean_file = os.path.join(timmean_path, file_name)
        run_shell_command(f"cdo -z zip -timmean {file} {timmean_file}", 60)

def cf_main():
    timmean_path = "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/timmean"
    scenarios=["historical","ssp370-GWL2K","ssp370-GWL3K"]
    files = [
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/cf_wind_historical.nc",
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/cf_wind_ssp370-GWL2K.nc",
        "/work/bb1203/g260190_heinrich/Dunkelflaute/Data/250m/cf_wind_ssp370-GWL3K.nc",
    ]

    for scenario, file in zip(scenarios,files):
        file_name = os.path.basename(file)
        timmean_file = os.path.join(timmean_path, file_name)
        run_shell_command(f"cdo -z zip -timmean {file} {timmean_file}", 60)

if __name__ == "__main__":
    cf_main()