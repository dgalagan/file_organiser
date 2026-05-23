import pandas as pd
from utils.json import load_json, save_json

json_data = load_json("ref\\extension_metadata.json")
ext_mapping = pd.read_excel("ref\\ext_mapping.xlsx")

required_keys = ["magic_number", "software", "description", "category"]

#prepare keys
for required_key in required_keys:
    for key, value in json_data.items():
        if required_key not in value:
            
            if required_key == "magic_number":
                json_data[key][required_key] = {}
            
            json_data[key][required_key] = ""

for idx, row in ext_mapping.iterrows():
    ext = row["FileExtension"]
    if ext not in json_data:
        json_data[ext] = {
            "magic_number": {},
            "software": row["Software"],
            "description": row["Description"],
            "category": row["Category"] 
        }
    else:
        for required_key in required_keys:
            if required_key.title() in row:
                if required_key in json_data[ext]:
                    if not json_data[ext][required_key]:
                        json_data[ext][required_key] = row[required_key.title()]
                else:
                    json_data[ext][required_key] = row[required_key.title()]

save_json("ref\\file_extension_signatures.json", json_data)