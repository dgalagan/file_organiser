from core.transformation import DateParser, get_worksheets_count, label_duplicate, get_country, get_min_year, calc_full_hash, build_path, fill_na_from_col
from dataframe.pipeline import Pipeline, AssignTags, FilterCols, FilterRows, Compute, Transform
from dataframe.col_filter import KeywordFilter, NameFilter, TagFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor
from dataframe.predicate import Condition, And
import pandas as pd
from reverse_geocoder import RGeocoder
from utils.path import is_not_dir, get_normalized_path, get_dir_depth, get_branch_depth, get_file_extension
from utils.text import uppercase_text, strip_text

def user_input_pipeline():
    return Pipeline(
            [
                Transform(ElementProcessor(get_normalized_path), NameFilter("DirPath")),
                Compute(ElementProcessor(is_not_dir), NameFilter("DirPath"), "isInvalid"),
                Compute(ColProcessor(pd.DataFrame.duplicated), NameFilter("DirPath"), "isDuplicate"),
                FilterRows(And([Condition("isInvalid", "eq", False), Condition("isDuplicate", "eq", False)])),
                Compute(ElementProcessor(get_dir_depth), NameFilter("DirPath"), "DirDepth"),
                Compute(ElementProcessor(get_branch_depth), NameFilter("DirPath"), "BranchDepth"),
                Compute(RowProcessor(lambda r: r["BranchDepth"] - r["DirDepth"]), NameFilter(["BranchDepth", "DirDepth"]), "BranchDepthFromDir"),
            ]
        )

def exif_pipeline(date_parser: DateParser, geocoder: RGeocoder):
    return Pipeline(
        [
            AssignTags(KeywordFilter(["createdate", "creationdate", "datetimeoriginal", "datetimedigitized"]), ["created_dt"]), #  "createddatetime", "datetimecreated", "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
            AssignTags(NameFilter(["ID3:Year", "EXE:TimeStamp", "XMP:Timestamp", "PNG:ExifDateTime", "Composite:GPSDateTime", "QuickTime:PurchaseDate"]), ["created_dt"]),
            AssignTags(KeywordFilter(["accessdate", "lastplayed", "lastprinted"]), ["access_dt"]),
            AssignTags(KeywordFilter(["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"]), ["modify_dt"]),
            AssignTags(NameFilter(["SourceFile", "File:FileName", "File:FileSize", "File:FileTypeExtension", "XML:HeadingPairs", "EXIF:GPSLatitude", "EXIF:GPSLongitude", "EXIF:Model"]), ["required"]),
            FilterCols(TagFilter(["created_dt", "access_dt", "modify_dt", "required"])),
            Compute(ColProcessor(pd.DataFrame.duplicated, keep=False), NameFilter("File:FileSize"), "IsSizeDuplicate"),
            Compute(ElementProcessor(calc_full_hash), NameFilter("SourceFile"), "ContentHash", where=Condition("IsSizeDuplicate", "eq", True)),
            Compute(ColProcessor(pd.DataFrame.duplicated), NameFilter("ContentHash"), "IsContentDuplicate", where=Condition("IsSizeDuplicate", "eq", True)),
            Transform(ColProcessor(pd.DataFrame.fillna, value=False), NameFilter("IsContentDuplicate")),
            Compute(ElementProcessor(label_duplicate), NameFilter("IsContentDuplicate"), "DuplicateLabel"),
            Transform(ElementProcessor(date_parser.parse), TagFilter(["created_dt", "modify_dt"])),
            Compute(RowProcessor(get_min_year), TagFilter(["created_dt", "modify_dt"]), "Year"),
            Compute(ElementProcessor(get_file_extension), NameFilter("SourceFile"), "FileExtension"),
            Transform(ElementProcessor(strip_text, char_to_remove="."), NameFilter("FileExtension")),
            Transform(ElementProcessor(uppercase_text), NameFilter("FileExtension")),
            Transform(RowProcessor(fill_na_from_col, from_col="FileExtension", to_col="File:FileTypeExtension"), NameFilter(["File:FileTypeExtension", "FileExtension"])),
            Compute(ElementProcessor(get_worksheets_count, target_headings=["Worksheets", "Листы"]), NameFilter("XML:HeadingPairs"), "CountWorksheets"),
            Compute(RowProcessor(get_country, geocoder=geocoder, lat_col="EXIF:GPSLatitude", lon_col="EXIF:GPSLongitude"), NameFilter(["EXIF:GPSLatitude", "EXIF:GPSLongitude"]), "Country"),
            FilterRows(Condition("File:FileTypeExtension", 'ne', 'MRIMGX'))
        ]
    )

def build_path_pipeline(dest_dir: str, path_components: list[str]):
    return Pipeline(
        [
            Transform(ColProcessor(pd.DataFrame.fillna, value="Other"), NameFilter("Category")),
            Compute(RowProcessor(build_path, dest_dir=dest_dir), NameFilter(path_components), "DestDir")
        ]
    )