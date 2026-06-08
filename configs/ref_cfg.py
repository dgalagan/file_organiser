import os
from configs.env_cfg import PROJECT_DIR

# Define storage directory path
ref_dir = "ref"
ref_dir_path = os.path.join(PROJECT_DIR, ref_dir)

# Define storage filenames
EXTENSION_MAPPING_NAME = "extension_mapping.json"
ref_names = [EXTENSION_MAPPING_NAME]

REFS_LOCATION = {
    ref_name: os.path.join(ref_dir_path, ref_name)
    for ref_name in ref_names
}