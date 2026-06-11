from configs.env_cfg import EXIF_DB_NAME, HASH_DB_NAME
from core.extraction import extract_exif_data, extract_hash_data

#########       EXTRACTION PARAMETERS       #########
exif_params = ["-j", "-G"]
include_tags = ["-all"]
exclude_tags = ["--File:Directory"]
exif_args = [*exif_params, *include_tags, *exclude_tags]

#########           CONFIGURATION           #########
EXIF_CFG = {"args": exif_args, "batch_size": 50}
HASH_CFG = {"hash_algo": "sha256", "parts": 8, "read_cap": 1024}
DB_CALC = {
    EXIF_DB_NAME: {"cfg": EXIF_CFG, "cfg_str": "".join(EXIF_CFG["args"]),"func": extract_exif_data},
    HASH_DB_NAME: {"cfg": HASH_CFG, "cfg_str": "".join(str(value) for value in HASH_CFG.values()), "func": extract_hash_data}
}

DB_INIT_CFG = {
    EXIF_DB_NAME: {},
    HASH_DB_NAME: {}
}

DB_RESET_FLAGS = {
    EXIF_DB_NAME: False,
    HASH_DB_NAME: False
}
