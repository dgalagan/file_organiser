from configs.cli_cfg import cli_objects, cli_grouped_objects
from configs.env_cfg import TARGET_DIR
from configs.ref_cfg import REFS_LOCATION
from configs.storage_cfg import STORAGES_LOCATION, STORAGES_INIT, STORAGES_RESET
from configs.extraction_cfg import EXTRACTION_CFG
from configs.transformation_cfg import COLUMNS_ALIASES, PIPELINE
from cli.renderer import render_cli_object
from core.env_setup import prepare_target_dir
from core.dir_input import get_user_dirs
from core.processing_scope import collect_files_to_organise
from core.df_processor import DfProcessor
import os
import pandas as pd
import sys
import shutil
from tqdm import tqdm
from utils.text import uppercase_text
from utils.json import init_json, load_json, save_json, reset_json
from utils.path import is_file

def main():
    
    #########        SETUP ENV       #########
    
    is_ready = prepare_target_dir(TARGET_DIR)
    if not is_ready:
        return 1
    
    #########       USER INPUT       #########
    
    try:
        input_dirs = get_user_dirs(cli_grouped_objects, cli_objects)
    except Exception as e:
        print(e)

    #########    PROCESSING SCOPE    #########
    
    try:
        dirs, files_to_organise = collect_files_to_organise(input_dirs, cli_objects=cli_objects)
    except Exception as e:
        print(e)

    #########      LOAD STORAGE      #########
    
    # Load storages
    storages = {}
    for storage_name, storage_path in STORAGES_LOCATION.items():
        # Initialize storage file
        if not is_file(storage_path):
            try:
                init_cfg = STORAGES_INIT[storage_name]
                init_json(storage_path, **init_cfg)
            except Exception as e:
                print(e)
        # Reset the storage file if requested
        if STORAGES_RESET[storage_name]:
            try:
                reset_json(storage_path)
            except Exception as e:
                print(e)
        # Load storage data
        try:
            storage_data = load_json(storage_path)
            storages[storage_name] = storage_data
        except Exception as e:
            print(e)

    #########      EXTRACT DATA      #########
    
    # Initialize runtime data containers
    processing_queue = {storage_name: [] for storage_name in storages}
    runtime_data = {storage_name: {} for storage_name in storages}
    runtime_size = {}
    runtime_mtime = {}

    # Prepare files processing queues
    tqdm_desc = "Prepare files processing queue:"
    for storage_name, storage_data in storages.items():
        for file in tqdm(files_to_organise, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
            # Get current mtime and size
            runtime_size[file] = os.path.getsize(file)
            runtime_mtime[file] = os.path.getmtime(file)
            # Get cached data
            file_history = storage_data.get(file, {})
            cached_snapshot = file_history.get(EXTRACTION_CFG[storage_name]["cfg_str"], {})
            cached_data = cached_snapshot.get("data")
            cached_mtime = cached_snapshot.get("mtime")
            cached_size = cached_snapshot.get("size")
            # Add file to processing queue or reuse cached data for this runtime
            if not file_history or not cached_snapshot or runtime_mtime[file] != cached_mtime or runtime_size[file] != cached_size:
                processing_queue[storage_name].append(file)
            else:
                runtime_data[storage_name][file] = cached_data       
        # Log the total number of files that require processing
        print(f"{len(processing_queue[storage_name])} file(s) queued for [{storage_name}] processing")

    print(render_cli_object(cli_objects["divider"]))
    # Handle processing queue
    for storage_name, files_to_process in processing_queue.items():
        # Load storage
        storage_data = storages.get(storage_name, {})
        # Load storage config
        extraction_cfg = EXTRACTION_CFG.get(storage_name, {})
        cfg_str = extraction_cfg.get("cfg_str", '')
        cfg = extraction_cfg.get("cfg", {})
        func = extraction_cfg.get("func")
        if cfg_str and cfg and func is not None:
            for processed_file, data in func(files_to_process, cfg):
                # Update runtime container
                runtime_data[storage_name][processed_file] = data
                # Update storage
                if processed_file not in storage_data:
                    storage_data[processed_file] = {cfg_str: {"data":data, "mtime":runtime_mtime[processed_file], "size":runtime_size[processed_file]}}
                else:
                    storage_data[processed_file][cfg_str] = {"data":data, "mtime":runtime_mtime[processed_file], "size":runtime_size[processed_file]}
        # Save updated data
        try:
            save_json(STORAGES_LOCATION[storage_name], storage_data)
        except Exception as e:
            print(e)
    
    print(render_cli_object(cli_objects["divider"]))
    #########     TRANSFORM DATA     #########
    
    # Load ref data into df
    refdata_dfs = {}
    for ref_name, ref_path in REFS_LOCATION.items():
        try:
            refdata = load_json(ref_path)
            refdata_df = pd.DataFrame.from_dict(refdata, orient="index")
            refdata_df = refdata_df.rename(columns=COLUMNS_ALIASES[ref_name]).rename(uppercase_text, axis="index")
            refdata_dfs[ref_name] = refdata_df
        except Exception as e:
            print(e)

    # Load runtime data into df
    metadata_dfs = {}
    for runtime_key, runtime_dict in runtime_data.items():
        metadata_df = pd.DataFrame.from_dict(runtime_dict, orient="index")
        metadata_df = metadata_df.rename(columns=COLUMNS_ALIASES[runtime_key])
        metadata_df = DfProcessor(metadata_df).run_pipeline(PIPELINE[runtime_key]).df
        metadata_dfs[runtime_key] = metadata_df

    # Concat metadata dfs
    full_metadata = pd.concat(metadata_dfs.values(), axis=1)

    # Join refdata
    refdata_df = refdata_dfs["extension_mapping.json"]
    enriched_df = pd.merge(full_metadata, refdata_df[["Category"]], how="left", left_on="FileExtension", right_index=True)
    target_path_df = DfProcessor(enriched_df).run_pipeline(PIPELINE["target_path"]).df

    print(render_cli_object(cli_objects["divider"]))
    #########     CHECK DISK SPACE    #########
    
    files_size = target_path_df["FileSize"].sum()
    _, _, free = shutil.disk_usage(TARGET_DIR)
    if files_size >= free:
        print(f"Not enough space to move files: free {int(free /(1<<30))} GB, required {int(files_size /(1<<30))} GB")
        return 1
    
    ########        MOVE FILES        #########
    
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