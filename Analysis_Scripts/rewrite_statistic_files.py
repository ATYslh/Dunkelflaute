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

def rewrite_file(input_file):
    load_json_file()

if __name__ == "__main__":
    input_file="Analysis_Scripts/Wind/Duisburg.json"