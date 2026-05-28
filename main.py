from configs.storage_cfg import STORAGES_LOCATION, STORAGES_CFG, STORAGES_RESET, EXIF_STORAGE_NAME, HASH_STORAGE_NAME
from configs.hash_cfg import HASH_CFG
from configs.exif_cfg import EXIF_CFG, BATCH_SIZE
from core.input_handling import setup_environment, get_user_input
from core.scanning import get_scope
from core.metadata import calc_file_hash, get_batches, get_exif_metadata
from core.exif_data import DateParser, get_worksheets_count, get_year, label_duplicate
from core.df_processor import DfProcessor
from exiftool import ExifTool
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

    #########        EXTRACT         #########
    loaded_storages = {}
    for storage_name, storage_path in STORAGES_LOCATION.items():
        # Initialize storage file
        if not is_file(storage_path):
            try:
                storage_cfg = STORAGES_CFG[storage_name]
                init_json(storage_path, **storage_cfg)
            except Exception as e:
                print(e)
        # Reset the storage file if requested
        if STORAGES_RESET[storage_name]:
            try:
                reset_json(storage_path)
            except Exception as e:
                print(e)
        # Load storage into runtime
        try:
            loaded_data = load_json(storage_path)
            loaded_storages[storage_name] = loaded_data
        except Exception as e:
            print(e)

    # Load storages
    exif_storage = loaded_storages[EXIF_STORAGE_NAME]
    hash_storage = loaded_storages[HASH_STORAGE_NAME]
    
    # Serialize configuration settings into strings for hashing
    hash_cfg_str = "".join(str(value) for value in HASH_CFG.values())
    exif_cfg_str = "".join(EXIF_CFG)

    # Initialize runtime containers
    runtime_hashes = {}
    runtime_exif = []
    runtime_exif_fails = []
    runtime_size = {}
    runtime_mdate = {}

    # Initialize lists for file processing
    files_to_hash = []
    files_to_exif = []

    # Prepare processing lists
    tqdm_desc = "Prepare processing lists"
    for file in tqdm(files, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        
        # Get stored hash data
        stored_hashes = hash_storage.get(file, {})
        stored_hash = stored_hashes.get(hash_cfg_str, {})
        stored_h_mtime = stored_hash.get("mtime")
        stored_h_size = stored_hash.get("size")

         # Get stored exif data
        stored_exifs = exif_storage.get(file, {})
        stored_exif = stored_exifs.get(exif_cfg_str, {})
        stored_e_mtime = stored_exif.get("mtime")
        stored_e_size = stored_exif.get("size")
    
        # Get current mtime and size
        runtime_size[file] = os.path.getmtime(file)
        runtime_mdate[file] = os.path.getsize(file)
    
        # Fill files list to calculate hash
        if not stored_hashes or not stored_hash or runtime_size[file] != stored_h_mtime or runtime_mdate[file] != stored_h_size:
            files_to_hash.append(file)
        else:
            runtime_hashes[file] = hash_storage[file][hash_cfg_str]["hash"]

        # Fill files list to extract exif
        if not stored_exifs or not stored_exif or runtime_size[file] != stored_e_mtime or runtime_mdate[file] != stored_e_size:
            files_to_exif.append(file)
        else:
            runtime_exif.append(exif_storage[file][exif_cfg_str]["exif"])

    print(f"{len(files_to_hash)} to calculate hash, {len(files_to_exif)} to extract exif")
    
    # Calc hash and update storage
    tqdm_desc = "Extract hash data and update storage"
    for file_to_hash in tqdm(files_to_hash, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        file_hash = calc_file_hash(file_to_hash, **HASH_CFG)
        # Update runtime dict
        runtime_hashes[file_to_hash] = file_hash
        # Update storage
        if file_to_hash not in hash_storage:
            hash_storage[file_to_hash] = {hash_cfg_str: {"hash":file_hash, "mtime":runtime_mdate[file_to_hash], "size":runtime_size[file_to_hash]}}
        else:
            hash_storage[file_to_hash][hash_cfg_str] = {"hash":file_hash, "mtime":runtime_mdate[file_to_hash], "size":runtime_size[file_to_hash]}

    # Calc exif and update storage
    tqdm_desc = "Extract exif data and update storage"
    batches = get_batches(files_to_exif, batch_size=BATCH_SIZE)
    with ExifTool(encoding="utf-8") as et:
        for batch in tqdm(batches, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
            batch_results = get_exif_metadata(batch, et, EXIF_CFG)
            runtime_exif.extend(batch_results)
            # Update exif storage
            for idx, file_result in enumerate(batch_results):
                file_path = file_result.get("SourceFile", "").replace('/', os.sep)
                if file_path:
                    if file_path not in exif_storage:
                        exif_storage[file_path] = {exif_cfg_str: {"exif": file_result, "mtime":runtime_mdate[file_path], "size":runtime_size[file_path]}}
                    else:
                        exif_storage[file_path][exif_cfg_str] = {"exif": file_result, "mtime":runtime_mdate[file_path], "size":runtime_size[file_path]}
                else:
                    runtime_exif_fails.append(batch[idx])

    # Save updated data
    for storage_name, updated_storage in loaded_storages.items():
        try:
            save_json(STORAGES_LOCATION[storage_name], updated_storage)
        except Exception as e:  
            print(e)

    #########       TRANSFORM        #########
    print(f"LOAD & TRANSFORM EXIF DATA")
    try:
        exif_processor = DfProcessor()
        (
            exif_processor
            .load_dict(runtime_exif, orient="columns")
            .transform(os.path.normpath, col_names="SourceFile")
            .transform(DateParser().parse, col_keywords=created_dt_tags)
            .transform(lower_text, col_names="File:FileTypeExtension")
            .compute(pd.Series.min, func_mode="row", store_col="AggTimestamp", col_keywords=created_dt_tags)
            .compute(get_year, store_col="Year", col_names="AggTimestamp")
            .compute(get_worksheets_count, store_col="CountExcelWorksheets", col_names="XML:HeadingPairs")
            # .compute(get_worksheets_count, store_col="Location", col_names=["EXIF:GPSLatitude", "EXIF:GPSLongitude"])
            .set_index("SourceFile")
        )
    except Exception as e:
        print(f"{e} while processing exif")
    
    # read basic
    print(f"LOAD & TRANSFORM HASH DATA")
    try:
        hash_processor = DfProcessor()
        (
            hash_processor
            .load_dict(runtime_hashes, orient="index", cols=["Hash"])
            .compute(pd.Series.duplicated, func_mode="col", store_col="isDuplicate", col_names="Hash")
            .compute(label_duplicate, store_col="DuplicateStatus", col_names="isDuplicate")
        )
    except Exception as e:
        print(f"{e} while processing hash")

    # read category mapping
    print(f"LOAD & TRANSFORM REF DATA")
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