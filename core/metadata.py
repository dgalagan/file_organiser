from exiftool import ExifTool
import hashlib
import os
from utils.json import init_json, load_json_str
from utils.path import is_file

def get_exif_metadata(batch: list[str], et, et_cfg) -> list[dict] | list:
    try:
        raw_output = et.execute(*et_cfg, *batch)
        return load_json_str(raw_output)
    except Exception as e:
        print(e)
        return []

def get_batches(files: list[str], batch_size=None):
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

def init_storage(storage_cfg: dict, storage_path: str, storage_reset: list = None) -> dict:
    
    # Init report container
    report = {"initialized": [], "skipped": [], "error": []}
    storage_reset = storage_reset or []

    # Create storage path if not exist
    os.makedirs(storage_path, exist_ok=True)

    # Init/reset individual storage files
    for file_name, config in storage_cfg.items():
        # Assemble storage file location
        file_path = os.path.join(storage_path, file_name)
        # Create storage if not exist
        if not is_file(file_path) or file_name in storage_reset:
            try:
                init_json(file_path, **config)
                report["initialized"].append(file_path)
            except Exception as e:
                report["error"].append((file_path, e))
        else:
            report["skipped"].append(file_path)

    return report

def calc_file_hash(path, hash_algo, parts, read_cap):
    hash_func = getattr(hashlib, hash_algo)
    file_size = os.path.getsize(path)
    file_parts = file_size // parts
    # remainder = file_size % parts
    byte_steps = [file_parts * step for step in range(parts)]
    combined_hash = hash_func()
    try:
        with open(path, "rb") as f:
            for byte_step in byte_steps:
                f.seek(byte_step, 0)
                data = f.read(read_cap)
                combined_hash.update(data)
        return combined_hash.hexdigest()
    except PermissionError:
        return None