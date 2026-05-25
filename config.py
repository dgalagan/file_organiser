import os

# ==============================================================================
# 1. STORAGE CONFIGURATION
# ==============================================================================
EXIF_STORAGE = "exif_metadata.json"
BASIC_STORAGE = "basic_metadata.json"
STORAGE_DIR = "db"

STORAGE_CFG = {
    EXIF_STORAGE: {
        "container": [],
        "encoding": "utf-8",
        "indent": 4,
    },
    BASIC_STORAGE: {
        "container": {},
        "encoding": "utf-8",
        "indent": 4,
    }
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
EXIF_CFG = {
}