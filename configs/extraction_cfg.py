from configs.storage_cfg import EXIF_STORAGE_NAME, HASH_STORAGE_NAME
from core.extraction import extract_exif_data, extract_hash_data

#########       EXIF       #########
exif_params = ["-j", "-G"]
include_tags = ["-all"]
exclude_tags = ["--File:Directory"]
EXIF_ARGS = [*exif_params, *include_tags, *exclude_tags]
BATCH_SIZE = 0
EXIF_CFG = {
    "args": EXIF_ARGS,
    "batch_size": BATCH_SIZE
}

#########       HASH       #########
HASH_CFG = {
    "hash_algo": "sha256",
    "parts": 8,
    "read_cap": 1024
}

#########       DATA       #########
EXTRACTION_CFG = {
    EXIF_STORAGE_NAME: {
        "cfg": EXIF_CFG,
        "cfg_str": "".join(EXIF_CFG["args"]),
        "func": extract_exif_data,
    },
    HASH_STORAGE_NAME: {
        "cfg": HASH_CFG,
        "cfg_str": "".join(str(value) for value in HASH_CFG.values()),
        "func": extract_hash_data,
    }
}