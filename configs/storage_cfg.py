import os
from configs.hash_cfg import HASH_CFG
from configs.exif_cfg import EXIF_CFG
from core.metadata import get_hash_data, get_exif_data

# Define storage directory path
project_dir =  os.getcwd()
storage_dir = "db"
storage_path = os.path.join(project_dir, storage_dir)

# Ensure the storage directory exists
os.makedirs(storage_path, exist_ok=True)

# Define storage filenames
EXIF_STORAGE_NAME = "exif_db.json"
HASH_STORAGE_NAME = "hash_db.json"
storage_names = [EXIF_STORAGE_NAME, HASH_STORAGE_NAME]

STORAGES_LOCATION = {
    storage_name: os.path.join(storage_path, storage_name)
    for storage_name in storage_names
}

STORAGES_INIT_CFG = {
    EXIF_STORAGE_NAME: {
        "container": {},
        "encoding": "utf-8",
        "indent": 4,
    },
    HASH_STORAGE_NAME: {
        "container": {},
        "encoding": "utf-8",
        "indent": 4,
    }
}

STORAGES_RESET = {
    EXIF_STORAGE_NAME: False,
    HASH_STORAGE_NAME: False
}

STORAGES_RUNTIME_CFG = {
    EXIF_STORAGE_NAME: {"cfg": EXIF_CFG, "cfg_str": "".join(EXIF_CFG)},
    HASH_STORAGE_NAME: {"cfg": HASH_CFG, "cfg_str": "".join(str(value) for value in HASH_CFG.values())}
}

STORAGES_PIPELINE_CFG = {
    EXIF_STORAGE_NAME: {
        "cfg": EXIF_CFG,
        "cfg_str": "".join(EXIF_CFG["args"]),
        "func": get_exif_data,
    },
    HASH_STORAGE_NAME: {
        "cfg": HASH_CFG,
        "cfg_str": "".join(str(value) for value in HASH_CFG.values()),
        "func": get_hash_data,
    }
}