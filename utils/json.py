import json

def init_json(json_path: str, container: list | dict | None = None, encoding: str | None = None, indent: int = 4) -> None:
    with open(json_path, "w", encoding=encoding) as f:
        json.dump(container, f, indent=indent)
    print(f"JSON file initialized: {json_path}")

def load_json(json_path: str):
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[Error] {json_path} not found")
        return None
    except json.JSONDecodeError:
        print(f"[Error] {json_path} is corrupted or not a valid JSON.")
        return None
    except PermissionError:
        print(f"[Error] Access denied to {json_path}")
        return None
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        return None

def load_json_str(json_str: str):
    return json.loads(json_str)

def save_json(json_path: str, json_data: dict):
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=4, ensure_ascii=False)
            print(f"{json_path} saved successfully!")
    except PermissionError:
        print(f"[Error] You don't have permission to write to {json_path}")
    except TypeError as e:
        print(f"[Error] Data contains non-JSON serializable objects: {e}")
    except OSError as e:
        print(f"[Error] System/Disk error occurred: {e}")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
