import os
from configs.env_cfg import PROJECT_DIR

# Define storage directory path
storage_dir = "db"
storage_dir_path = os.path.join(PROJECT_DIR, storage_dir)

# Ensure the storage directory exists
os.makedirs(storage_dir_path, exist_ok=True)

# Define storage filenames
EXIF_STORAGE_NAME = "exif_db.json"
HASH_STORAGE_NAME = "hash_db.json"
storage_names = [EXIF_STORAGE_NAME, HASH_STORAGE_NAME]

STORAGES_LOCATION = {
    storage_name: os.path.join(storage_dir_path, storage_name)
    for storage_name in storage_names
}

STORAGES_RESET = {
    storage_name: False
    for storage_name in storage_names
}

STORAGES_INIT = {
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