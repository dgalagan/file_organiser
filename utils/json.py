import json

def load_json(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(json_path: str, json_data: dict) -> None:
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=4, ensure_ascii=False)

def load_json_str(json_str: str):
    return json.loads(json_str)