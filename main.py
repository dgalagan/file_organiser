from config import STORAGE_CFG, STORAGE_DIR, EXIF_STORAGE, HASH_STORAGE, HASH_CFG
from core.input_handling import setup_environment, get_user_input
from core.scanning import get_scope
from core.metadata import init_storage, calc_file_hash, get_batches, get_exif_metadata
from core.exif_data import DateParser, get_worksheets_count, get_year
from core.df_processor import DfProcessor
from exiftool import ExifTool
import os
import pandas as pd
import sys
import shutil
from tqdm import tqdm
from utils.text import lower_text
from utils.json import load_json, save_json

# manage lowercase path cases

TARGET_DIR = "D:\\MyOrganizedFiles\\"
SAVE_REPORT = True
# Exif
INCLUDE_TAGS = ["-all"]
EXCLUDE_TAGS = ["--File:Directory"]
EXIF_PARAMS = ["-j", "-G"]
EXIF_CFG = [*EXIF_PARAMS, *INCLUDE_TAGS, *EXCLUDE_TAGS]

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

target_path_features = ["DuplicateStatus", "category", "Year", "EXIF:Model", "CombinedFileExtension", "CountExcelWorksheets", "Name"]

def label_duplicate(value):
    return "duplicate" if value else "original"

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
    
    # Init storage files
    project_dir = os.path.dirname(os.path.abspath(__file__))
    storage_path = os.path.join(project_dir, STORAGE_DIR)
    report = init_storage(STORAGE_CFG, storage_path)
    
    # Init runtime containers
    runtime_hashes = {}
    runtime_exif = []
    runtime_exif_fails = []
    runtime_size = {}
    runtime_mdate = {}

    # Load hash data
    hash_storage_path = os.path.join(storage_path, HASH_STORAGE)
    hash_storage = load_json(hash_storage_path)
    hash_cfg_str = "".join(str(value) for value in HASH_CFG.values())
    calc_hash = []
    
    # Load exif data
    exif_storage_path = os.path.join(storage_path, EXIF_STORAGE)
    exif_storage = load_json(exif_storage_path)
    exif_cfg_str = "".join(EXIF_CFG)  
    extract_exif = []

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
        current_mtime = os.path.getmtime(file)
        current_size = os.path.getsize(file)
    
        # Prepare files list to calculate hash
        if not stored_hashes or not stored_hash or current_mtime != stored_h_mtime or current_size != stored_h_size:
            calc_hash.append(file)
            runtime_size[file] = current_size
            runtime_mdate[file] = current_mtime
        else:
            runtime_hashes[file] = hash_storage[file][hash_cfg_str]["hash"]

        # Prepare files list to extract exif
        if not stored_exifs or not stored_exif or current_mtime != stored_e_mtime or current_size != stored_e_size:
            extract_exif.append(file)
        else:
            runtime_exif.append(exif_storage[file][exif_cfg_str]["exif"])

    print(f"{len(calc_hash)} to calculate hash, {len(extract_exif)} to extract exif")
    
    # Calc hash and update storageWW
    tqdm_desc = "Extract hash data and update storage"
    for file_to_hash in tqdm(calc_hash, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
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
    batches = get_batches(extract_exif, batch_size=100)
    with ExifTool(encoding="utf-8") as et:
        for batch in tqdm(batches, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
            batch_results = get_exif_metadata(batch, et, EXIF_CFG)
            runtime_exif.extend(batch_results)
            # Update exif storage
            for idx, file_result in enumerate(batch_results):
                file_path = file_result.get("SourceFile", "").replace('/', '\\')
                if file_path:
                    if file_path not in exif_storage:
                        exif_storage[file_path] = {exif_cfg_str: {"exif": file_result, "mtime":runtime_mdate[file_path], "size":runtime_size[file_path]}}
                    else:
                        exif_storage[file_path][exif_cfg_str] = {"exif": file_result, "mtime":runtime_mdate[file_path], "size":runtime_size[file_path]}
                else:
                    runtime_exif_fails.append(batch[idx])

    if calc_hash:
        save_json(hash_storage_path, hash_storage)
    if extract_exif:
        save_json(exif_storage_path, exif_storage)
    
    #########           ETL           #########
    # try:
    #     exif_processor = DfProcessor()
    #     (
    #         exif_processor
    #         .load_json("db\\exif_metadata.json", orient="records")
    #         .transform(os.path.normpath, col_names="SourceFile")
    #         .transform(DateParser().parse, col_keywords=created_dt_tags)
    #         .transform(lower_text, col_names="File:FileTypeExtension")
    #         .compute(pd.Series.min, func_mode="row", store_col="AggTimestamp", col_keywords=created_dt_tags)
    #         .compute(get_year, store_col="Year", col_names="AggTimestamp")
    #         .compute(get_worksheets_count, store_col="CountExcelWorksheets", col_names="XML:HeadingPairs")
    #         # .compute(get_worksheets_count, store_col="Location", col_names=["EXIF:GPSLatitude", "EXIF:GPSLongitude"])
    #         .set_index("SourceFile")
    #     )
    # except Exception as e:
    #     print(f"{e} while processing exif")
    
    # # read basic
    # try:
    #     basic_processor = DfProcessor()
    #     (
    #         basic_processor
    #         .load_json("db\\basic_metadata.json", orient="index")
    #         .compute(pd.Series.duplicated, func_mode="col", store_col="isDuplicate", col_names="Hash")
    #         .compute(label_duplicate, store_col="DuplicateStatus", col_names="isDuplicate")
    #     )
    # except Exception as e:
    #     print(e)

    # # read category mapping
    # try:
    #     category_processor = DfProcessor()
    #     (
    #         category_processor
    #         .load_json("db\\extension_metadata.json", orient="index")
    #     )
    # except Exception as e:
    #     print(e)

    # try:
    #     master_df = pd.DataFrame(index=pd.Index(list(files), name="FilePath"))
    #     master_df = master_df.join(
    #         [
    #             basic_processor.df[["Hash", "DuplicateStatus", "Name", "Size", "Ext"]],
    #             exif_processor.df[["File:FileTypeExtension", "CountExcelWorksheets", "Year", "EXIF:Model", "ID3:Year"]]
    #         ],
    #         how="left"
    #     )
    #     master_df["CombinedFileExtension"] = master_df["File:FileTypeExtension"].fillna(master_df["Ext"])
    #     master_df = pd.merge(master_df, category_processor.df[["category"]], how="left", left_on="CombinedFileExtension", right_index=True)
    #     master_df["TargetPath"] = master_df[target_path_features].apply(lambda row: TARGET_DIR + "\\".join([str(value) for value in row if pd.notna(value)]), axis=1)
    #     if SAVE_REPORT:
    #         report_path = TARGET_DIR + "\\" + "report.csv"
    #         master_df.to_csv(report_path, encoding="utf-8-sig")
    # except Exception as e:
    #     print(e)
    
    #########     CHECK DISK SPACE    #########
    
    # files_size = master_df["Size"].sum()
    # _, _, free = shutil.disk_usage(TARGET_DIR)
    # if files_size >= free:
    #     print(f"Not enough space to move files: free {int(free /(1<<30))} GB, required {int(files_size /(1<<30))} GB")
    #     return 1
    
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