import datetime as dt
import json
import os
import pandas as pd

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

# Mapping table
EXT_MAPPING = "ref\\ext_mapping.xlsx"

def select_cols_by_names(df: pd.DataFrame, names=[]):
    
    if not names:
        return df.copy()
    
    return df.filter(items=names).copy()


def assemble_target_path(files: list[str], target_dir: str, save_report=True) -> pd.DataFrame:
    ######    INIT     ######
    master_df = pd.DataFrame(index=pd.Index(list(files), name="FilePath"))
    
    # Consolidate data from different sources
    master_df = master_df.join(
        [
            basic_meta_df[["Hash", "DuplicateStatus", "Name", "Size", "Ext"]],
            exif_meta_df[["FileExtension", "CountExcelWorksheets", "AggTimestamp", "EXIF:Model"]],
        ],
        how="left"
    )
    master_df["CombinedFileExtension"] = master_df["FileExtension"].fillna(master_df["Ext"])
    master_df = master_df.join(category_df[["Category"]], on="FileExtension", how="left")
    master_df["TargetPath"] = master_df.apply(
        lambda x: 
        os.path.join(*[str(p) for p in [
            target_dir,
            x["DuplicateStatus"],
            x["Category"],
            x["Year"],
            x["EXIF:Model"], 
            x["CombinedFileExtension"],
            x["CountExcelWorksheets"],
            x["Name"]
        ] if pd.notna(p)]), axis=1)

    ######    LOAD     ######
    if save_report:
        report_path = target_dir + "\\" + "report.csv"
        master_df.to_csv(report_path, encoding="utf-8-sig")
    return master_df

def calculate_coverage(json_path: str):
    exif_meta = json.load(json_path)
    coverage_report = {}
    for exif_dict in exif_meta:
        for feature, value in exif_dict.items():
            if feature == "File:FileTypeExtension":
                if value in coverage_report:
                    coverage_report[value].append(exif_dict)
                else:
                    coverage_report[value] = [exif_dict]    
    return coverage_report