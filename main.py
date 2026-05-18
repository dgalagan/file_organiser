from core.input_handling import setup_environment, get_user_input
from core.scanning import get_scope
from core.metadata import run_metadata_extraction
from core.exif_data import DateParser, get_worksheets_count
from core.df_processor import DfProcessor, DfProcessorEXP
import pandas as pd
import sys
import datetime as dt
import os
import shutil
from utils.text import lower_text


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
# Mapping table
EXT_MAPPING = "ref\\ext_mapping.xlsx"

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
exif_cols = ["File:FileTypeExtension", "EXIF:Model", "EXIF:GPSLatitude", "EXIF:GPSLongitude", "XML:HeadingPairs", "ID3:Year"]

##### DateParse #####
# datetime patterns
dt_patterns = {
    "ddddsddsdd":                       "%Y{s0}%m{s1}%d",
    "ddddsddsddwddsdd":                 "%Y{s0}%m{s1}%d %H{s2}%M",
    "ddddsddsddwddsddl":                "%Y{s0}%m{s1}%d %H{s2}%MZ",
    "ddddsddsddwddsddsdd":              "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S",
    "ddddsddsddlddsddsddl":             "%Y{s0}%m{s1}%dT%H{s2}%M{s3}%SZ",
    "ddddsddsddwddsddsddl":             "%Y{s0}%m{s1}%d %H{s2}%M{s3}%SZ",
    "ddddsddsddwddsddsddsdd":           "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
    "ddddsddsddwddsddsddsddd":          "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
    "ddddsddsddwddsddsddsddsdd":        "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S%z",
    "ddddsddsddwddsddsddsdddsddsdd":    "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z"
}
# datetime null patterns
dt_nulls = ["0000:00:00 00:00:00", "0000:01:01 00:00:00", "1980:00:00 00:00:00", "1980:01:01 00:00:00"]
# datetime parser 
dt_parser = DateParser(dt_patterns, dt_nulls)

basic_transform = {
    "isDuplicate":          {"mode": "series", "func": lambda s: s.duplicated()},
    "DuplicateStatus":      {"mode": "value", "func": lambda value: "original" if not value else "duplicate"}
}

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
        failed_files = run_metadata_extraction(files, STORAGE_CFG, EXIF_CFG, HASH_CFG, reset_storage=False, batch_size=100)
        if failed_files:
            print(f"{len(failed_files)} failed files identified")
            # Remove failed files from files
            files = files - failed_files
    except Exception as e:
        print(e)
    
    #########           ETL           #########
    ######   EXTRACT   ######
    # read exif
    try:
        exif_processor = DfProcessorEXP()
        (
            exif_processor
            .load_json("db\\exif_metadata.json", orient="records")
            .transform(os.path.normpath, col_names=["SourceFile"])
            .transform(lambda value: dt_parser.parse(value) if isinstance(value, str) else None, col_keywords=created_dt_tags)
            .transform(lambda value: lower_text(value) if isinstance(value, str) else None, col_names=["File:FileTypeExtension"])
            .compute(pd.Series.duplicated, func_mode="series", store_col="isDuplicate", col_names=["SourceFile"])
            .compute(pd.Series.min, func_mode="row", store_col="AggTimestamp", col_keywords=created_dt_tags)
            .compute(lambda value: dt.datetime.fromtimestamp(value).year, func_mode="element", store_col="Year", col_names=["AggTimestamp"])
            .compute(get_worksheets_count, store_col="CountExcelWorksheets", col_names=["XML:HeadingPairs"])
        )
        print(exif_processor.df[["SourceFile", "File:FileTypeExtension", "AggTimestamp", "Year", "CountExcelWorksheets"]]) 
    except Exception as e:
        print(f"{e} while processing exif")
    
    # read basic
    try:
        basic_meta_df = pd.read_json("db\\basic_metadata.json", orient="index")
    except Exception as e:
        print(e)

    # read category mapping
    try:
        category_df = pd.read_excel(EXT_MAPPING, index_col="FileExtension")
    except Exception as e:
        print(e)

    try:
        master_df = pd.DataFrame()
    except Exception as e:
        print(e)
    
    #########     CHECK DISK SPACE    #########
    
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