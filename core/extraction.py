from configs.env_cfg import ENCODING, EXECUTABLE_PATHS
from exiftool import ExifTool
import hashlib
import json
import os
from tqdm import tqdm

def get_batches(files: list[str], batch_size: int) -> list[list[str]]:
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

def extract_exif_data(files: list[str], config: dict):
    
    if not isinstance(config, dict):
        raise TypeError(f"Unsupported config type - {type(config)}")
    
    args = config.get("args", [])
    batch_size = config.get("batch_size", 0)

    batches = None
    if batch_size > 0:
        batches = get_batches(files, batch_size)
    
    tqdm_desc = "Extract exif data:"
    with ExifTool(encoding=ENCODING, executable=EXECUTABLE_PATHS["exif"]) as et:
        if batches is not None:
            for batch in tqdm(batches, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
                raw_output = et.execute(*args, *batch)
                try:
                    batch_results = json.loads(raw_output)
                except Exception as e:
                    batch_results = []
                for file_result in batch_results:
                    file = file_result.get("SourceFile", "").replace('/', os.sep)
                    yield file, file_result
        else:
            for file in tqdm(files, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
                raw_output = et.execute(*args, file)
                try:
                    file_result = json.loads(raw_output)
                except Exception as e:
                    file_result = {}
                if isinstance(file_result, list) and len(file_result) == 1:
                    yield file, file_result[0]
                elif isinstance(file_result, dict):
                    yield file, file_result
                else:
                    print("Unsupported output from exif")
                    yield file, file_result

def calc_file_hash(path: str, hash_algo: str, parts: int, read_cap: int) -> dict:
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
        return {"hash": combined_hash.hexdigest()}
    except PermissionError:
        return {}
    
def extract_hash_data(files: list[str], config: dict):

    if not isinstance(config, dict):
        raise TypeError(f"Unsupported config type - {type(config)}")

    tqdm_desc = "Extract hash data:"
    for file in tqdm(files, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        file_hash = calc_file_hash(file, **config)
        yield file, file_hash