from exiftool import ExifTool
import hashlib
import os
from tqdm import tqdm
from utils.json import init_json, load_json, load_json_str, save_json
from utils.path import is_file, get_file_stat

def get_file_hash(path, hash_algo, parts, read_cap):
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

def get_exif_metadata(batch: list[str], et, et_cfg) -> list[dict] | list:
    try:
        raw_output = et.execute(*et_cfg, *batch)
        return load_json_str(raw_output)
    except Exception as e:
        print(e)
        return []

def get_batches(files: list[str], batch_size=None):
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

def run_metadata_extraction(files: set[str], storage_cfg, exif_cfg, hash_cfg, reset_storage=False, batch_size=None):
    ########     TQDM BAR     ########
    b_format = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
    ########   INIT STORAGE   ########
    storage = {}
    for path, config in storage_cfg.items():
        if not is_file(path) or reset_storage:
            container = config.get("structure")
            encoding = config.get("encoding")
            indent = config.get("indent")
            init_json(path, container=container, encoding=encoding, indent=indent)
            storage[path] = container
        else:
            storage[path] = load_json(path)
    
    ########   LOAD STORAGE   ########
    exif_metadata = storage.get("db\\exif_metadata.json", [])
    basic_metadata = storage.get("db\\basic_metadata.json", {})
    hash_cfg_str = "".join(str(value) for value in hash_cfg.values())
    exif_cfg_str = "".join(exif_cfg)
    
    ######## FAILED FILES ########
    failed_files = set()

    ######## BASIC METADATA + HASH ########
    new_files = files.difference(set(basic_metadata.keys()))
    files_to_exif = new_files.copy()
    desc = "Acquiring basic metadata + Hash"
    for file in tqdm(files, desc=f"{desc:<35}", bar_format=b_format):
        # Get stored values
        stored_basic_metadata =  basic_metadata.get(file, {})
        stored_mtime = stored_basic_metadata.get("ModifiedAt")
        stored_size = stored_basic_metadata.get("Size")
        stored_hash_cfg = stored_basic_metadata.get("HashConfig", "")
        stored_exif_cfg = stored_basic_metadata.get("ExifConfig", "")
        # Get current values
        current_mtime = os.path.getmtime(file)
        current_size = os.path.getsize(file)
        # Calculate change conditions
        is_new = file in new_files
        is_modified = current_size != stored_size or abs(current_mtime - stored_mtime) > 0.1
        is_hash_cfg_changed = hash_cfg_str != stored_hash_cfg
        is_exif_cfg_changed = exif_cfg_str != stored_exif_cfg
        # Evaluate conditions
        if is_new:
            file_meta = get_file_stat(file)
            file_hash = get_file_hash(file, **hash_cfg)
            if not file_meta or file_hash is None:
                failed_files.add(file)
            basic_metadata[file] = {
                **file_meta,
                "Hash": file_hash,
                "HashConfig": hash_cfg_str,
                "ExifConfig": exif_cfg_str
            }
        elif is_modified:
            file_meta = get_file_stat(file)
            file_hash = get_file_hash(file, **hash_cfg)
            if not file_meta or file_hash is None:
                failed_files.add(file)
            basic_metadata[file].update({
                **file_meta,
                "Hash": file_hash,
            })
            if is_hash_cfg_changed:
                basic_metadata[file].update({"HashConfig": hash_cfg_str})
            if is_exif_cfg_changed:
                basic_metadata[file].update({"ExifConfig": exif_cfg_str})
            files_to_exif.add(file)
        else:
            if is_hash_cfg_changed:
                file_hash = get_file_hash(file, **hash_cfg)
                if file_hash is None:
                    failed_files.add(file)
                basic_metadata[file].update({
                    "HashConfig": hash_cfg_str,
                    "Hash": file_hash
                })
            if is_exif_cfg_changed:
                basic_metadata[file].update({
                    "ExifConfig": exif_cfg_str
                })
                files_to_exif.add(file)
    
    ######## EXIF METADATA ########
    # Clean up exif metadata
    if files_to_exif and exif_metadata:
        desc = "Clean up outdated exif entries"
        for file_metadata in tqdm(exif_metadata, desc=f"{desc:<35}", bar_format=b_format):
            if file_metadata["SourceFile"].replace('/', '\\') in files_to_exif:
                exif_metadata.remove(file_metadata)
            continue
    
    # Extract exif metadata
    desc = "Acquiring exif metadata"
    batches = get_batches(list(files_to_exif), batch_size=batch_size)
    with ExifTool(encoding="utf-8") as et:
        for batch in tqdm(batches, desc=f"{desc:<35}", bar_format=b_format):
            batch_results = get_exif_metadata(batch, et, exif_cfg)
            exif_metadata.extend(batch_results)
            # Catch failed file
            if {} in batch_results:
                failed_files_idx = [idx for idx, exif_dict in enumerate(batch_results) if not exif_dict]
                for failed_file_idx in failed_files_idx:
                    failed_files.add(batch[failed_file_idx])
    
    for path, data in storage.items():
        save_json(path, data)

    print(f"--------------------------------------------------------------------------------")
    
    return failed_files