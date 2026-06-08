from configs.exif_cfg import EXIF_CFG
from configs.hash_cfg import HASH_CFG
from configs.storage_cfg import EXIF_STORAGE_NAME, HASH_STORAGE_NAME
from core.extraction import extract_exif_data, extract_hash_data

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