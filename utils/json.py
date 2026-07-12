from configs.env_cfg import JSON_SETTINGS
import json

def load_json(json_path: str):
    with open(json_path, "r", encoding=JSON_SETTINGS["encoding"]) as f:
        return json.load(f)

def save_json(json_path: str, json_data: dict) -> None:
    with open(json_path, "w", encoding=JSON_SETTINGS["encoding"]) as f:
        json.dump(json_data, f, indent=JSON_SETTINGS["indent"], ensure_ascii=JSON_SETTINGS["ensure_ascii"])

def load_json_str(json_str: str):
    return json.loads(json_str)

def calculate_coverage(json_path: str):
    exif_meta = json.load(json_path)
    coverage_report = {}
    for exif_dict in exif_meta:
        for feature, value in exif_dict.items():
            if feature == "File:FileTypeExtension":
                if value in coverage_report:
                    coverage_report[value].append(exif_dict)
                else:
                    coverage_report[value] = [exif_dict]    
    return coverage_report