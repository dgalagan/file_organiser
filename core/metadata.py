from exiftool import ExifTool
import hashlib
from utils.path import is_file, get_file_stat
from utils.text import split_text
from tqdm import tqdm
import datetime
import os
import json

def read_bytes(path, count=None):
    with open(path, "rb") as f:
        header = f.read(count)
        return header.hex().upper()

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
        print(f"[Error] {json_path} is corrupted orr not a valid JSON.")
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
        return {"Hash": combined_hash.hexdigest()}
    except PermissionError:
        return {}

def is_file_changed(path: str, stored_metadata: dict) -> bool:
    if not stored_metadata:
        # Empty dict means 'new' file arrived
        return True
    current_mtime = os.path.getmtime(path)
    current_size = os.path.getsize(path)
    stored_mtime = normalize_exif_datetime(stored_metadata.get("File:FileModifyDate"))
    stored_size = stored_metadata.get("File:FileSize")
    # Check for size or modified time differences
    if abs(current_mtime - stored_mtime) > 1 or current_size != stored_size:
        return True
    
    return False

def get_exif_metadata(batch: list[str], et, et_cfg) -> list[dict] | list:
    try:
        raw_output = et.execute(*et_cfg, *batch)
        batch_results = json.loads(raw_output)
        return batch_results
    except Exception as e:
        return []

def get_batches(files: list[str], batch_size=None):
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

def normalize_exif_datetime(raw_date, separator=" "):
    if separator in raw_date:
        date_elements = split_text(raw_date, separator=separator)
        date_elements[0] = date_elements[0].replace(":", "-")
        iso_date = separator.join(date_elements)
        return datetime.datetime.fromisoformat(iso_date).timestamp()
    iso_date = raw_date[:10].replace(":", "-") + raw_date[10:0]
    return datetime.datetime.fromisoformat(iso_date).timestamp()

def run_metadata_extraction(files: list[str], storage_cfg, exif_cfg, hash_cfg, reset_storage=False, batch_size=None):
    # Tqdm bar format 
    b_format = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
    # Initialize new json storage or load existing
    for path, config in storage_cfg.items():
        exists = is_file(path)
        container = config.get("structure")
        encoding = config.get("encoding")
        indent = config.get("indent")
        if not exists or reset_storage:
            init_json(path, container=container, encoding=encoding, indent=indent)

    # Load stored data
    files_metadata = load_json("db\\files_metadata.json")
    stored_exif_cfg = load_json("db\\exif_cfg.json")
    stored_hash_cfg = load_json("db\\hash_cfg.json")

    # Select updated and new files
    files_to_process = []
    desc = "Select new and updated files"
    for file in tqdm(files, desc=f"{desc:<35}", bar_format=b_format):
        stored_metadata = files_metadata.get(file, {})
        if is_file_changed(file, stored_metadata):
            files_to_process.append(file)
            files_metadata[file] = {}
    
    latest_hash_cfg = None
    if stored_hash_cfg:
        latest_cfg_mtime = max(stored_hash_cfg.keys(), key=float)
        latest_hash_cfg = stored_hash_cfg.get(latest_cfg_mtime, {})
    if latest_hash_cfg == hash_cfg:
        print("Hash config unchanged. Processing only new/modified files.")
        target_files = files_to_process
    else:
        print("Hash config changed or new. Re-processing all files.")
        stored_hash_cfg[datetime.datetime.today().timestamp()] = hash_cfg
        target_files = files
    
    desc = "Getting hashes"
    for file_to_process in tqdm(target_files, desc=f"{desc:<35}", bar_format=b_format):
        file_hash = get_file_hash(file_to_process, **hash_cfg)
        files_metadata[file_to_process].update(file_hash)

    desc = "Getting metadata"
    latest_exif_cfg = None
    if stored_exif_cfg:
        latest_cfg_mtime = max(stored_exif_cfg.keys(), key=float)
        latest_exif_cfg = stored_exif_cfg.get(latest_cfg_mtime, [])
    if latest_exif_cfg == exif_cfg:
        print("Exif config unchanged. Processing only new/modified files.")
        target_files = files_to_process
    else:
        print("Exif config changed or new. Re-processing all files.")
        target_files = files
        stored_exif_cfg[datetime.datetime.today().timestamp()] = exif_cfg
    
    desc = "Getting metadata"
    batches = get_batches(target_files, batch_size=batch_size)
    for batch in tqdm(batches, desc=f"{desc:<35}", bar_format=b_format):
        with ExifTool(encoding="utf-8") as et:
            batch_results = get_exif_metadata(batch, et, exif_cfg)
            for exif_dict in batch_results:
                source_file = exif_dict.pop("SourceFile").replace("/", "\\")
                files_metadata[source_file].update(exif_dict)

    if latest_hash_cfg is not None or latest_exif_cfg is not None:
        save_json("db\\files_metadata.json", files_metadata)
        save_json("db\\hash_cfg.json", stored_hash_cfg)
        save_json("db\\exif_cfg.json", stored_exif_cfg)