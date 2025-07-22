import json

def generate_filename(folder:str)->str:
    parts = folder.split("/")
    return "_".join([parts[4], parts[6], parts[7], parts[9], parts[-2]])

def calculate_capacity_factors():
    with open('your_file.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
