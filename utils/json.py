import json

def init_json(json_path: str, container: list | dict,  encoding: str | None = None, indent: int | None = None) -> None:
    try:
        with open(json_path, "w", encoding=encoding) as f:
            json.dump(container, f, indent=indent)
    except IOError as e:
        print(f"Disk Error on {json_path}: {e}")
        raise
    except TypeError as e:
        print(f"Data formatting error for {json_path}: {e}")
        raise

def load_json(json_path: str):
    try:
        with open(json_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[Error] {json_path} not found")
        raise
    except json.JSONDecodeError:
        print(f"[Error] {json_path} is corrupted or not a valid JSON.")
        raise
    except PermissionError:
        print(f"[Error] Access denied to {json_path}")
        raise
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        raise

def save_json(json_path: str, json_data: dict):
    try:
        with open(json_path, "w") as f:
            json.dump(json_data, f)
            print(f"{json_path} saved successfully!")
    except PermissionError:
        print(f"[Error] You don't have permission to write to {json_path}")
        raise
    except TypeError as e:
        print(f"[Error] Data contains non-JSON serializable objects: {e}")
        raise
    except OSError as e:
        print(f"[Error] System/Disk error occurred: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        raise

def reset_json(json_path: str):
    try:
        json_data = load_json(json_path)
        json_data.clear()
        save_json(json_path, json_data)
    except PermissionError:
        print(f"[Error] You don't have permission to write to {json_path}")
        raise
    except TypeError as e:
        print(f"[Error] Data contains non-JSON serializable objects: {e}")
        raise
    except OSError as e:
        print(f"[Error] System/Disk error occurred: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        raise

def load_json_str(json_str: str):
    return json.loads(json_str)