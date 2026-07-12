from configs.env_cfg import EXIF_DB_NAME, HASH_DB_NAME, EXTENSION_REF_NAME
from core.transformation import DateParser, get_worksheets_count, get_year, label_duplicate, get_country, get_min_year, build_path
import pandas as pd
from utils.path import is_not_dir, get_normalized_path, get_dir_depth, get_branch_depth
from df_worker import Context, Transform, Compute, FilterCols, NameFilter, TagFilter, FilterRows, Condition, And, ElementProcessor, RowProcessor, ColProcessor

COLUMN_ALIASES = {
    EXIF_DB_NAME: {
        "File:FileName"             : "FileName",
        "File:FileSize"             : "FileSize",
        "File:FileTypeExtension"    : "FileExtension",
        "XML:HeadingPairs"          : "DocumentStructure",
        "EXIF:Model"                : "CameraModel",
        "EXIF:GPSLatitude"          : "Latitude",
        "EXIF:GPSLongitude"         : "Longitude",
    },
    HASH_DB_NAME: {
        "hash"                      : "Hash",
    },
    EXTENSION_REF_NAME: {
        "magic_number"              : "MagicNumber",
        "software"                  : "Software",
        "description"               : "Desc",
        "category"                  : "Category",
    }
}

date_parser = DateParser()

tag_cfg = {
    "created_dt": {
        "keywords": ["createdate", "creationdate", "datetimeoriginal", "datetimedigitized"], #  "createddatetime", "datetimecreated", "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
        "items": ["ID3:Year", "EXE:TimeStamp", "XMP:Timestamp", "PNG:ExifDateTime", "Composite:GPSDateTime", "QuickTime:PurchaseDate"]
    },
    "access_dt": {
        "keywords": ["accessdate", "lastplayed", "lastprinted"],
    },
    "modify_dt": {
        "keywords": ["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"],
    },
    "required": {
        "items": ["File:FileName", "File:FileSize", "File:FileTypeExtension", "XML:HeadingPairs", "EXIF:GPSLatitude", "EXIF:GPSLongitude", "EXIF:Model"]
    }
}

PIPELINES = {
    EXIF_DB_NAME: {
        "tag_cfg": tag_cfg,
        "steps": [
            FilterCols(TagFilter("build")),
            Transform(ElementProcessor(date_parser.parse), TagFilter(["created_dt", "modify_dt"])),
            Compute(RowProcessor(get_min_year), TagFilter(["created_dt", "modify_dt"]), "Year"),
            Compute(ElementProcessor(get_worksheets_count), NameFilter("XML:HeadingPairs"), "CountWorksheets"),
            Compute(RowProcessor(get_country, {"lat_col": "EXIF:GPSLatitude", "lon_col": "EXIF:GPSLongitude"}), NameFilter(["EXIF:GPSLatitude", "EXIF:GPSLongitude"]), "Country")
        ]
    },

    HASH_DB_NAME: {
        "steps": [
            Compute(ColProcessor(pd.Series.duplicated), NameFilter("hash"), "IsDuplicate"),
            Compute(ElementProcessor(label_duplicate), NameFilter("IsDuplicate"), "DuplicateLabel"),
        ]
    },
}