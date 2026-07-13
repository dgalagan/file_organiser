from configs.env_cfg import EXIF_DB_NAME, HASH_DB_NAME, EXTENSION_REF_NAME
from core.transformation import DateParser, get_worksheets_count, get_year, label_duplicate, get_country, get_min_year, build_path
import pandas as pd
from utils.path import is_not_dir, get_normalized_path, get_dir_depth, get_branch_depth
from dataframe.pipeline import Pipeline, AssignTags, FilterCols, Compute, Transform
from dataframe.col_filter import KeywordFilter, NameFilter, TagFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor

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

PIPELINES = {
    EXIF_DB_NAME: Pipeline(
        [
            AssignTags(KeywordFilter(["createdate", "creationdate", "datetimeoriginal", "datetimedigitized"]), ["created_dt"]), #  "createddatetime", "datetimecreated", "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
            AssignTags(NameFilter(["ID3:Year", "EXE:TimeStamp", "XMP:Timestamp", "PNG:ExifDateTime", "Composite:GPSDateTime", "QuickTime:PurchaseDate"]), ["created_dt"]),
            AssignTags(KeywordFilter(["accessdate", "lastplayed", "lastprinted"]), ["access_dt"]),
            AssignTags(KeywordFilter(["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"]), ["modify_dt"]),
            AssignTags(NameFilter(["File:FileName", "File:FileSize", "File:FileTypeExtension", "XML:HeadingPairs", "EXIF:GPSLatitude", "EXIF:GPSLongitude", "EXIF:Model"]), ["required"]),
            FilterCols(TagFilter(["created_dt", "access_dt", "modify_dt", "required"])),
            Transform(ElementProcessor(date_parser.parse), TagFilter(["created_dt", "modify_dt"])),
            Compute(RowProcessor(get_min_year), TagFilter(["created_dt", "modify_dt"]), "Year"),
            Compute(ElementProcessor(get_worksheets_count), NameFilter("XML:HeadingPairs"), "CountWorksheets"),
            Compute(RowProcessor(get_country, {"lat_col": "EXIF:GPSLatitude", "lon_col": "EXIF:GPSLongitude"}), NameFilter(["EXIF:GPSLatitude", "EXIF:GPSLongitude"]), "Country")
        ]
    ),

    HASH_DB_NAME: Pipeline(
        [
            Compute(ColProcessor(pd.DataFrame.duplicated), NameFilter("hash"), "IsDuplicate"),
            Compute(ElementProcessor(label_duplicate), NameFilter("IsDuplicate"), "DuplicateLabel"),
        ]
    )
}