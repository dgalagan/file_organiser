import os

# Define storage directory path
project_dir =  os.getcwd()
ref_dir = "ref"
ref_dir_path = os.path.join(project_dir, ref_dir)

# Define storage filenames
EXTENSION_MAPPING_NAME = "extension_mapping.json"
ref_names = [EXTENSION_MAPPING_NAME]

REFS_LOCATION = {
    ref_name: os.path.join(ref_dir_path, ref_name)
    for ref_name in ref_names
}