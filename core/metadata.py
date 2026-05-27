from exiftool import ExifTool
import hashlib
import os
from utils.json import load_json_str

def get_exif_metadata(batch: list[str], et, et_cfg) -> list[dict] | list:
    try:
        raw_output = et.execute(*et_cfg, *batch)
        return load_json_str(raw_output)
    except Exception as e:
        print(e)
        return []

def get_batches(files: list[str], batch_size=None):
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

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