"""
The purpose of this script is to rewrite the existing statistic files so that
they are not grouped by individual file names anymore but rather by the
general name for the dataset
"""

import json


def load_json_file(json_file: str) -> dict:
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


def write_json_file(filename: str, content: dict) -> None:
    with open(filename, "w") as file:
        json.dump(content, file, indent=4)


def find_matching_files(statistics, split_words, file_names):
    for key_again in statistics.keys():
        if "historical" in key_again:
            continue

        if all(word in key_again for word in split_words):
            file_names.append(key_again)
    
    return file_names


def rewrite_file(input_file):
    statistics = load_json_file(input_file)
    for key in statistics.keys():
        file_names = []
        if "historical" not in key:
            continue

        file_names.append(key)
        split_words = key.split("_historical_")

        file_names=find_matching_files(statistics, split_words, file_names)

        if len(file_names)==1:
            print(f"Found nothing for {key}")
        # merge


if __name__ == "__main__":
    input_file = "/work/bb1203/g260190_heinrich/Dunkelflaute/Analysis_Scripts/Wind/Duisburg.json"
    rewrite_file(input_file=input_file)
