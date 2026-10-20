import json

def load_json(path: str):
    with open(path, 'r') as f:
        return json.load(f)

def save_json(data, path: str):
    with open(path, 'w') as f:
        return json.dump(data, f)