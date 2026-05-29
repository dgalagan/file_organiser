from configs.extraction_cfg import EXTRACTION_CFG
from configs.ref_cfg import REFS_LOCATION
from configs.storage_cfg import STORAGES_LOCATION, STORAGES_INIT, STORAGES_RESET, EXIF_STORAGE_NAME, HASH_STORAGE_NAME
from core.input_handling import setup_environment, get_user_input
from core.scanning import get_scope
from core.transformation import DateParser, get_worksheets_count, get_year, label_duplicate
from core.df_processor import DfProcessor
import os
import pandas as pd
import sys
import shutil
from tqdm import tqdm
from utils.text import lower_text
from utils.json import init_json, load_json, save_json, reset_json
from utils.path import is_file

# manage lowercase path cases in manual input

TARGET_DIR = "D:\\MyOrganizedFiles\\"
SAVE_REPORT = True

# datetime tags
created_dt_tags = [
    # "exe:timestamp", # specific, actually holds date
    # "xmp:timestamp", # specific, actually holds date
    # "png:exifdatetime", # specific, actually holds date
    # "composite:gpsdatetime", # specific, actually holds date
    # "quicktime:purchasedate", # temporary, overlap with recognised receipt json 
    "createdate", # 18 instances
    "creationdate", # 7 instances
    "datetimeoriginal", # 8 instances
    "datetimedigitized", # 3 instances
    # "createddatetime", # 1 instance
    # "datetimecreated", # 1 instance
    # "encodingtime", # 2 instances
    # "profiledatetime", # 1 instance
    # "retaildate", # 2 instance
    # "ripdate", # 2 instance
    # "releasetime", # 2 instance
    # "originalreleaseyear", # 1 instance
]
access_dt_tags = [
    "accessdate",
    "lastplayed",
    "lastprinted",
]
modify_dt_tags = [
    "datemodify", # 1 instance
    "lastsaved", # 4 instance
    "lastupdated" # 0 instance
    "moddate", # 0 instance
    "modifydate", # 17 instance
    "metadatadate", # 2 instance
    "sourcemodified" # 2 instance
]

target_path_features = ["DuplicateStatus", "category", "Year", "EXIF:Model", "CombinedFileExtension", "CountExcelWorksheets", "File:FileName"]

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

    #########      LOAD STORAGE      #########
    
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
    runtime_data = {storage_name: {} for storage_name in storages}
    runtime_size = {}
    runtime_mtime = {}

    # Initialize processing queue container
    processing_queue = {storage_name: [] for storage_name in storages}

    # Prepare processing queues
    tqdm_desc = "Prepare processing queue:"
    for file in tqdm(files, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        # Get current mtime and size
        runtime_size[file] = os.path.getsize(file)
        runtime_mtime[file] = os.path.getmtime(file)  
        for storage_name, storage_data in storages.items():
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
    print("\n".join(f"{len(processing_queue[storage_name])} file(s) queued for [{storage_name}] processing" for storage_name in processing_queue))
    
    # Handle processing queue
    for storage_name, files_to_process in processing_queue.items():
        # Load storage
        storage = storages.get(storage_name, {})
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
                if processed_file not in storage:
                    storage[processed_file] = {cfg_str: {"data":data, "mtime":runtime_mtime[processed_file], "size":runtime_size[processed_file]}}
                else:
                    storage[processed_file][cfg_str] = {"data":data, "mtime":runtime_mtime[processed_file], "size":runtime_size[processed_file]}

    # Save updated data
    for storage_name, updated_data in storages.items():
        try:
            save_json(STORAGES_LOCATION[storage_name], updated_data)
        except Exception as e:  
            print(e)

    # Load ref data into runtime
    for ref_name, ref_path in REFS_LOCATION.items():
        try:
            ref_data = load_json(ref_path)
            runtime_data[ref_name] = ref_data
        except Exception as e:
            print(e)

    #########     TRANSFORM DATA     #########
    # Load runtime data into df
    runtime_dfs = {}
    for runtime_key, runtime_dict in runtime_data.items():
        df = pd.DataFrame.from_dict(runtime_dict, orient="index")
        runtime_dfs[runtime_key] = df

    print(f"TRANSFORM EXIF")
    try:
        exif_processor = DfProcessor()
        (
            exif_processor
            .load_dict(runtime_data[EXIF_STORAGE_NAME], orient="index")
            .transform(os.path.normpath, col_names="SourceFile")
            .transform(DateParser().parse, col_keywords=created_dt_tags)
            .transform(lower_text, col_names="File:FileTypeExtension")
            .compute(pd.Series.min, func_mode="row", store_col="AggTimestamp", col_keywords=created_dt_tags)
            .compute(get_year, store_col="Year", col_names="AggTimestamp")
            .compute(get_worksheets_count, store_col="CountExcelWorksheets", col_names="XML:HeadingPairs")
            # .compute(function needed, store_col="Location", col_names=["EXIF:GPSLatitude", "EXIF:GPSLongitude"])
            .set_index("SourceFile")
        )
    except Exception as e:
        print(f"{e} while processing exif")
    
    print(f"TRANSFORM HASH")
    try:
        hash_processor = DfProcessor()
        (
            hash_processor
            .load_dict(runtime_data[HASH_STORAGE_NAME], orient="index", cols=["Hash"])
            .compute(pd.Series.duplicated, func_mode="col", store_col="isDuplicate", col_names="Hash")
            .compute(label_duplicate, store_col="DuplicateStatus", col_names="isDuplicate")
        )
    except Exception as e:
        print(f"{e} while processing hash")

    print(f"TRANSFORM REF")
    try:
        category_processor = DfProcessor()
        (
            category_processor
            .load_json("ref\\extension_metadata.json", orient="index")
        )
    except Exception as e:
        print(f"{e} while processing ref data")

    print(f"PREPARE MASTER DATA")
    try:
        master_df = pd.DataFrame(index=pd.Index(list(files), name="FilePath"))
        master_df = master_df.join(
            [
                hash_processor.df[["Hash", "DuplicateStatus"]],
                exif_processor.df[["File:FileName", "File:FileSize", "File:FileTypeExtension", "CountExcelWorksheets", "Year", "EXIF:Model", "ID3:Year"]]
            ],
            how="left"
        )
        master_df["CombinedFileExtension"] = master_df["File:FileTypeExtension"]
        master_df = pd.merge(master_df, category_processor.df[["category"]], how="left", left_on="CombinedFileExtension", right_index=True)
        master_df["category"] = master_df["category"].fillna("Other")
        master_df["TargetPath"] = master_df[target_path_features].apply(lambda row: TARGET_DIR + os.sep.join([str(value) for value in row if pd.notna(value)]), axis=1)
        
        if SAVE_REPORT:
            report_path = TARGET_DIR + os.sep + "report.csv"
            master_df.to_csv(report_path, encoding="utf-8-sig")
        
    except Exception as e:
        print(f"{e} while processing master")
    
    #########     CHECK DISK SPACE    #########
    
    files_size = master_df["File:FileSize"].sum()
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