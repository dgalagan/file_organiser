from datetime import datetime
import pandas as pd
from utils.text import lower_text
import os
import json

# Mapping table
EXT_MAPPING = "ref\\ext_mapping.xlsx"

def get_cols_by_keywords(cols, keywords=[]):
    target_cols = set()
    for keyword in keywords:
        for col in cols:
            if keyword in col.lower():
                target_cols.add(col)
    return target_cols

def assemble_target_path(files: list[str], target_dir: str, save_report=True) -> pd.DataFrame:
    
    ######   EXTRACT   ######
    exif_meta_df = pd.read_json("db\\exif_metadata.json", orient="records").set_index("SourceFile")
    basic_meta_df = pd.read_json("db\\basic_metadata.json", orient="index")
    ext_mapping_df = pd.read_excel(EXT_MAPPING).set_index("FileExtension")
    master_df = pd.DataFrame(index=pd.Index(list(files), name="FilePath"))

    ######  TRANSFORM  ######
    # Prepare exif data
    exif_meta_df.index =  exif_meta_df.index.to_series().apply(lambda x: x.replace("/", "\\"))
    # exif_meta_df[""] = get_earliest_created_date()
   
    # Indexes transform
    exif_meta_df.index =  exif_meta_df.index.to_series().apply(lambda x: x.replace("/", "\\"))
    ext_mapping_df.index = ext_mapping_df.index.to_series().apply(lower_text)
    # Other transform
    master_df = master_df.join(
        [
            basic_meta_df[["Hash","Name","Size","ModifiedAt","Ext"]],
            exif_meta_df[["File:FileTypeExtension",
                          "EXIF:Model",
                          "EXIF:GPSLatitude",
                          "EXIF:GPSLongitude",
                          "XML:HeadingPairs",
                        #   "EXIF:CreateDate",
                        #   "XML:CreateDate",
                        #   "PDF:CreateDate",
                        #   "FlashPix:CreateDate",
                        #   "RTF:CreateDate",
                        #   "QuickTime:CreateDate",
                        #   "HTML:CreateDate",
                        #   "ASF:CreationDate",
                          "ID3:Year"
                          ]]
        ],  
        
        how="left"
    )
    master_df["isDuplicate"] = master_df["Hash"].duplicated()
    master_df["DuplicationStatus"] = master_df["isDuplicate"].apply(lambda x: "unique" if not x else "duplicate")
    master_df["Year"] = master_df["ModifiedAt"].apply(lambda x: datetime.fromtimestamp(x).year)
    master_df["CountWorksheetsIdx"] = master_df["XML:HeadingPairs"].apply(
        lambda x: next((i+1 for i, v in enumerate(x) if v in ["Worksheets", "Листы"]), "") if isinstance(x, list) else ''
    )
    master_df["CountWorksheets"] = master_df.apply(
        lambda x: x["XML:HeadingPairs"][x["CountWorksheetsIdx"]] if isinstance(x["CountWorksheetsIdx"], int) else '', axis=1
    )
    master_df["File:FileTypeExtension"] = master_df["File:FileTypeExtension"].fillna(master_df["Ext"]).apply(lower_text)
    master_df = master_df.join(ext_mapping_df[["Category"]], on="File:FileTypeExtension", how="left")
    master_df["TargetPath"] = master_df.apply(
        lambda x: 
        os.path.join(*[str(p) for p in [
            target_dir,
            x["DuplicationStatus"],
            x["Category"],
            x["Year"],
            x["EXIF:Model"],
            x["File:FileTypeExtension"],
            x["CountWorksheets"],
            x["Name"]
        ] if pd.notna(p)]), axis=1)

    ######   LOAD      ######
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