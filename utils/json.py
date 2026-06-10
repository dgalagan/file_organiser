from configs.env_cfg import ENCODING
import json

def init_json(json_path: str, container: list | dict, indent: int | None = None) -> None:
    with open(json_path, "w", encoding=ENCODING) as f:
        json.dump(container, f, indent=indent)

def load_json(json_path: str):
    with open(json_path, "r", encoding=ENCODING) as f:
        return json.load(f)

def save_json(json_path: str, json_data: dict):
    with open(json_path, "w", encoding=ENCODING) as f:
        json.dump(json_data, f)

def reset_json(json_path: str):
    json_data = load_json(json_path)
    json_data.clear()
    save_json(json_path, json_data)

def load_json_str(json_str: str):
    return json.loads(json_str)