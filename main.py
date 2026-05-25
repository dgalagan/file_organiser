from config import STORAGE_CFG, EXIF_DB_NAME, BASIC_DB_NAME, HASH_CFG
from core.input_handling import setup_environment, get_user_input
from core.scanning import get_scope
from core.metadata import init_storage, run_metadata_extraction
from core.exif_data import DateParser, get_worksheets_count, get_year
from core.df_processor import DfProcessor
import os
import pandas as pd
import sys
import shutil
from utils.text import lower_text

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
    
    # Initialize storage
    report = init_storage(STORAGE_CFG, storage_dir="db")
    print(report)

    # try:
    #     failed_files = run_metadata_extraction(files, STORAGE_CFG, EXIF_CFG, HASH_CFG, batch_size=100)
    #     if failed_files:
    #         print(f"{len(failed_files)} failed files identified")
    #         # Remove failed files from files
    #         files = files - failed_files
    # except Exception as e:
    #     print(e)
    
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