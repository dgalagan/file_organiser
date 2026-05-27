import os
# ==============================================================================
# 1. STORAGE CONFIGURATION
# ==============================================================================

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

STORAGES_CFG = {
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

# ==============================================================================
# 2. HASH CONFIGURATION
# ==============================================================================
HASH_CFG = {
    "hash_algo": "sha256",
    "parts": 8,
    "read_cap": 1024
}
# ==============================================================================
# 3. EXIFTOOL CONFIGURATION
# ==============================================================================
include_tags = ["-all"]
exclude_tags = ["--File:Directory"]
exif_params = ["-j", "-G"]
EXIF_CFG = [*exif_params, *include_tags, *exclude_tags]
# ==============================================================================
# 4. REF DATA PATH
# ==============================================================================
