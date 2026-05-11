from core.input_handling import setup_environment, get_user_input
from core.scanning import get_scope
from core.metadata import run_metadata_extraction
from core.etl import assemble_target_path
import pandas as pd
import sys
from datetime import datetime
import os
import shutil

TARGET_DIR = "D:\\MyOrganizedFiles"

# Storage
STORAGE_CFG = {
    "db\\exif_metadata.json": {
        "structure": [],
        "encoding": "utf-8",
        "indent": 4
    },
    "db\\basic_metadata.json": {
        "structure": {},
        "encoding": "utf-8",
        "indent": 4
    }
}
# Hash
HASH_CFG = {"hash_algo": "sha256", "parts": 8, "read_cap": 1024}
# Exif
INCLUDE_TAGS = ["-all"]
EXCLUDE_TAGS = ["--File:Directory"]
EXIF_PARAMS = ["-j", "-G"]
EXIF_CFG = [*EXIF_PARAMS, *INCLUDE_TAGS, *EXCLUDE_TAGS]


def main():
    
    #########        SETUP ENV       #########
    
    is_ready = setup_environment(TARGET_DIR)
    if not is_ready:
        return 1
    
    #########       USER INPUT       #########
    
    try:
        input_dirs = get_user_input()
        dirs, files = get_scope(input_dirs)
    except Exception as e:
        print(e)

    #########    EXTRACT METADATA    #########
    
    try:
        failed_files = run_metadata_extraction(files, STORAGE_CFG, EXIF_CFG, HASH_CFG, reset_storage=True, batch_size=100)
        if failed_files:
            print(f"{len(failed_files)} failed files identified")
            # Remove failed files from files
            files = files - failed_files
    except Exception as e:
        print(e)
    
    #########           ETL          #########
    
    try:
        master_df = assemble_target_path(files, TARGET_DIR)
    except Exception as e:
        print(e)
    
    ######## CHECK DISK SPACE ########
    
    files_size = master_df["Size"].sum()
    _, _, free = shutil.disk_usage(TARGET_DIR)
    if files_size >= free:
        print(f"Not enough space to move files: free {int(free /(1<<30))} GB, required {int(files_size /(1<<30))} GB")
        return 1
    
    ######## MOVE FILES ########
    
    # desc = "Copying files into new structure"
    # b_format = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
    # for index, row in tqdm(combined_df.iterrows(), total=len(combined_df), desc=f"{desc:<35}", bar_format=b_format):
    #     source = row["FilePath"]
    #     destination = row["TargetPath"]
    #     dest_dir = os.path.dirname(destination)
    #     if not os.path.exists(dest_dir):
    #         os.makedirs(dest_dir) 
    #     shutil.copy2(source, destination)
    
    return 0
    
if __name__ == "__main__":
    sys.exit(main())