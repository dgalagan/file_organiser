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