import datetime
from exiftool import ExifTool
import hashlib
import json
import os
from tqdm import tqdm
from utils.path import is_file, get_file_stat

OFFICE_SIGS = {
    4: [
        "504B0304",         # DOCX|PPTX|XLSX
        "FDFFFFFF10",       # XLS
        "FDFFFFFF22",       # XLS
        "FDFFFFFF23",       # XLS
        "FDFFFFFF28",       # XLS
        "FDFFFFFF29",       # XLS
        "FDFFFFFF1F",       # XLS
        "0D444F43",         # DOC
        "DBA52D00",         # DOC
        "ECA5C100",         # DOC
        "0F00E803",         # PPT    
        "006E1EF0",         # PPT
        "A0461DF0",         # PPT
    ],
    8: [
        "D0CF11E0A1B11AE1", # DOC|DOT|PPS|PPT|XLA|XLS|WIZ
        "504B030414000600", # DOCX|PPTX|XLSX
        "0908100000060500", # XLS
        "CF11E0A1B11AE100", # DOC
        "FDFFFFFF0E000000", # PPT
        "FDFFFFFF1C000000", # PPT
        "FDFFFFFF43000000", # PPT
    ]
}
PDF_SIGS = {
    4: ["25504446"]
}
MP3_SIGS = {
    2: ["FFFB", "FFF3", "FFF2"],
    3: ["494433"]
}
AVI_SIGS = {
    4: ["52494646"],
    8: ["415649204C495354"]
}
JPG_SIGS = {
    2: ["FFD8"],
    3: ["FFD8FF"],
}
ZIP_SIGS = {
    4: ["504B0304", "504B0506", "504B0708", "504B0304"],
    5: ["504B537058"],
    6: ["504B4C495445", "57696E5A6970"],
    8: ["504B030414000100"]
}
TAR_SIGS = {
    8: ["7573746172003030", "7573746172202000"]
}
CHECK_EXT = {
    ".xls": OFFICE_SIGS,
    ".xlsx": OFFICE_SIGS,
    ".xlsm": OFFICE_SIGS,
    ".xlsb": OFFICE_SIGS,
    ".doc": OFFICE_SIGS,
    ".docx": OFFICE_SIGS,
    ".ppt": OFFICE_SIGS,
    ".pptx": OFFICE_SIGS,
    ".pptm": OFFICE_SIGS,
    ".potx": OFFICE_SIGS,
    ".msg": OFFICE_SIGS,
    ".pdf": PDF_SIGS,
    ".avi": AVI_SIGS,
    ".jpg": JPG_SIGS,
    ".jpeg": JPG_SIGS,
    ".zip": ZIP_SIGS,
    ".tar": TAR_SIGS,
}
EXCL_EXT = [
    ".ac3",     # The first two octets of an AC-3 frame are always the synchronization word, which has the hex value 0x0B77
    ".srt",
    ".mrimgx",  # Macrium uses zstandard compression https://kbx.macrium.com/sitemanagerplatform/sitemanager-platform-new-features
    ".cached",
    ".tmp"
]
TXT_EXT = [
    ".txt",
    ".md",
    ".ini",
]

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

def init_json(json_path: str, container: list | dict | None = None, encoding: str | None = None, indent: int = 4) -> None:
    with open(json_path, "w", encoding=encoding) as f:
        json.dump(container, f, indent=indent)
    print(f"JSON file initialized: {json_path}")


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
        batch_results = json.loads(raw_output)
        return batch_results
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