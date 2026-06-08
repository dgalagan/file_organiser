from configs.storage_cfg import EXIF_STORAGE_NAME, HASH_STORAGE_NAME
from configs.ref_cfg import EXTENSION_MAPPING_NAME
from core.transformation import DateParser, get_worksheets_count, get_year, label_duplicate
import pandas as pd
from utils.path import is_not_dir, get_normalized_path, get_dir_depth, get_branch_depth

COLUMNS_ALIASES = {
    EXIF_STORAGE_NAME: {
        "File:FileName"             : "FileName",
        "File:FileSize"             : "FileSize", 
        "File:FileTypeExtension"    : "FileExtension",
        "XML:HeadingPairs"          : "DocumentStructure",
        "EXIF:Model"                : "CameraModel",
        "EXIF:GPSLatitude"          : "Latitude",
        "EXIF:GPSLongitude"         : "Longitude",
    },
    HASH_STORAGE_NAME: {
        "hash"                      : "Hash",
    },
    EXTENSION_MAPPING_NAME: {
        "magic_number"              : "MagicNumber",
        "software"                  : "Software",
        "description"               : "Desc",
        "category"                  : "Category",
    }
}

COLUMN_TAGS = {
    "created_dt": ["createdate", "creationdate", "datetimeoriginal", "datetimedigitized", ], # "exe:timestamp", "xmp:timestamp", "png:exifdatetime", "composite:gpsdatetime", "quicktime:purchasedate", "createddatetime", "datetimecreated", "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
    "access_dt": ["accessdate", "lastplayed", "lastprinted"],
    "modify_dt": ["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"]
}

PIPELINE = {
    "user_dirs": [
        {"op": "transform",   "func": (get_normalized_path, "element"),                                           "use_cols": "DirPath"},
        {"op": "compute",     "func": (is_not_dir, "element"),                  "calc_col": "isInvalid",          "use_cols": "DirPath"},
        {"op": "compute",     "func": (pd.Series.duplicated, "col"),            "calc_col": "isDuplicate",        "use_cols": "DirPath"},
        {"op": "filter_rows", "cond": {"col": "isInvalid",   "comparator": "==", "val": False, "mask_junc": "AND"}},
        {"op": "filter_rows", "cond": {"col": "isDuplicate", "comparator": "==", "val": False, "mask_junc": "AND"}},
        {"op": "compute",     "func": (get_dir_depth, "element"),               "calc_col": "DirDepth",           "use_cols": "DirPath"},
        {"op": "compute",     "func": (get_branch_depth, "element"),            "calc_col": "BranchDepth",        "use_cols": "DirPath"},
        {"op": "compute",     "func": (lambda r: r.iloc[0] - r.iloc[1], "row"), "calc_col": "BranchDepthFromDir", "use_cols": ["BranchDepth", "DirDepth"]},
    ],
    EXIF_STORAGE_NAME: [
        {"op": "transform",   "func": (DateParser().parse, "element"),                                            "use_keywords": COLUMN_TAGS["created_dt"]},
        {"op": "compute",     "func": (pd.Series.min, "row"),                    "calc_col": "AggTimestamp",      "use_keywords": COLUMN_TAGS["created_dt"]},
        {"op": "compute",     "func": (get_year, "element"),                     "calc_col": "Year",              "use_cols": "AggTimestamp"},
        {"op": "compute",     "func": (get_worksheets_count, "element"),         "calc_col": "CountWorksheets",   "use_cols": "DocumentStructure"},
    ],
    HASH_STORAGE_NAME: [
        {"op": "compute",     "func": (pd.Series.duplicated, "col"),             "calc_col": "IsDuplicate",       "use_cols": "Hash"},
        {"op": "compute",     "func": (label_duplicate, "element"),              "calc_col": "DuplicateLabel",    "use_cols": "IsDuplicate"},
    ]
}
# .compute(function needed, store_col="Location", col_names=["EXIF:GPSLatitude", "EXIF:GPSLongitude"])