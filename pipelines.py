from core.transformation import get_worksheets_count, label_duplicate, get_country, get_min_year, calc_full_hash, build_path, fill_na_from_col
from dataframe.pipeline import Pipeline, AssignTags, FilterCols, FilterRows, Compute, Transform
from dataframe.col_filter import KeywordFilter, NameFilter, TagFilter, CombinedFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor
from dataframe.predicate import Condition, And, Or
from dataframe.context import Context
import pandas as pd
from utils.path import is_not_dir, get_normalized_path, depth_from_drive, subtree_depth
from utils.text import uppercase_text, strip_text, lstrip_text
import os
import hashlib

def get_ext(filename: str, uppercase: bool = False, separator: str = "."):
    # remove leading and trailing dots
    filename = strip_text(filename, char_to_remove=separator)
    name, ext = os.path.splitext(filename)
    return uppercase_text(lstrip_text(ext, char_to_remove=separator)) if uppercase else lstrip_text(ext, char_to_remove=separator)

def calc_cache_key(dev: str, ino: str, exif_args: list[str] | None = None) -> str:
    exif_args = exif_args or []
    parts = f"{dev}|{ino}|{"|".join(sorted(exif_args))}"
    return hashlib.md5(parts.encode()).hexdigest()


def prepare_dirs():
    return Pipeline(
            [
                Transform(ElementProcessor(get_normalized_path), NameFilter("DirPath")),
                Compute(ElementProcessor(is_not_dir), NameFilter("DirPath"), dest_col="IsInvalid"),
                Compute(ColProcessor(pd.DataFrame.duplicated, keep="first"), NameFilter("DirPath"), dest_col="IsDuplicate"),
                FilterRows(And([Condition("IsInvalid", "eq", False), Condition("IsDuplicate", "eq", False)])),
            ]
        )

def add_depth_metrics():
    return Pipeline(
            [
                Compute(ElementProcessor(depth_from_drive), NameFilter("DirPath"), dest_col="DirDepth"),
                Compute(ElementProcessor(subtree_depth), NameFilter("DirPath"), dest_col="SubtreeDepth"),
            ]
        )

def add_file_path(prefix: str = ""):
    return Pipeline(
        [
            Compute(RowProcessor(lambda row: os.path.join(row[f"{prefix}FileDir"], row["FileName"])), NameFilter([f"{prefix}FileDir", "FileName"]), dest_col=f"{prefix}FilePath"),
        ]
    )

def add_path_exists(prefix: str = ""):
    return Pipeline(
        [
            Compute(ElementProcessor(os.path.exists), NameFilter(f"{prefix}FilePath"), dest_col=f"{prefix}PathExists"),
        ]
    )

def add_files_stat(prefix: str = ""):
    return Pipeline(
            [
                Compute(ElementProcessor(os.stat), NameFilter(f"{prefix}FilePath"), dest_col=f"{prefix}FileStat", where=Condition(f"{prefix}PathExists", "eq", True)),
                Compute(ElementProcessor(lambda s: s.st_size), NameFilter(f"{prefix}FileStat"), dest_col=f"{prefix}Size", where=Condition(f"{prefix}PathExists", "eq", True)),
                Compute(ElementProcessor(lambda s: s.st_mtime), NameFilter(f"{prefix}FileStat"), dest_col=f"{prefix}ModifiedAt", where=Condition(f"{prefix}PathExists", "eq", True)),
                Compute(ElementProcessor(lambda s: s.st_dev), NameFilter(f"{prefix}FileStat"), dest_col=f"{prefix}InodeDev", where=Condition(f"{prefix}PathExists", "eq", True)),
                Compute(ElementProcessor(lambda s: s.st_ino), NameFilter(f"{prefix}FileStat"), dest_col=f"{prefix}Inode", where=Condition(f"{prefix}PathExists", "eq", True)),
            ]
        )

def add_cache_key(exif_args: list[str], prefix: str = ""):
    return Pipeline(
        [
            Compute(RowProcessor(lambda row: calc_cache_key(row[f"{prefix}InodeDev"], row[f"{prefix}Inode"], exif_args)), NameFilter([f"{prefix}InodeDev", f"{prefix}Inode"]), dest_col=f"{prefix}CacheKey"),
        ]
    )

def select_completed():
    return Pipeline(
        [
            FilterRows(Condition("Status", "eq", "COMPLETED"))
        ]
    )

def select_skipped():
    return Pipeline(
        [
            FilterRows(Condition("Status", "eq", "SKIPPED"))
        ]
    )

def select_failed():
    return Pipeline(
        [
            FilterRows(Condition("Status", "eq", "ERROR"))
        ]
    )

def tag_columns(ctx: Context):
    return Pipeline(
        [
            # "createddatetime"
            # "datetimecreated"
            # "encodingtime"
            # "profiledatetime"
            # "retaildate"
            # "ripdate"
            # "releasetime"
            # "originalreleaseyear"
            AssignTags(KeywordFilter(["createdate", "creationdate", "datetimeoriginal", "datetimedigitized"]), ["created_dt"]),
            AssignTags(NameFilter(["ID3:Year", "EXE:TimeStamp", "XMP:Timestamp", "PNG:ExifDateTime", "Composite:GPSDateTime", "QuickTime:PurchaseDate"]), ["created_dt"]),
            AssignTags(KeywordFilter(["accessdate", "lastplayed", "lastprinted"]), ["access_dt"]),
            AssignTags(KeywordFilter(["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"]), ["modify_dt"]),
        ],
        context=ctx
    )

def select_columns(ctx: Context):
    return Pipeline(
        [
            FilterCols(
                CombinedFilter(
                    [
                        NameFilter(["SourceFile", "File:FileName", "File:FileSize", "File:FileTypeExtension", "XML:HeadingPairs", "EXIF:GPSLatitude", "EXIF:GPSLongitude", "EXIF:Model"]),
                        TagFilter(["created_dt", "access_dt", "modify_dt"])
                    ]
                )
            ),
        ],
        context=ctx
    )

def resolve_extension(ctx: Context):
    return Pipeline(
        [
            Compute(ElementProcessor(get_ext, uppercase=True), NameFilter("File:FileName"), "ExtFromName"),
            Compute(RowProcessor(fill_na_from_col, from_col="ExtFromName", to_col="File:FileTypeExtension"), NameFilter(["File:FileTypeExtension", "ExtFromName"]), "Extension"),
        ],
        context=ctx
    )

def select_extension(ctx: Context):
    return Pipeline(
        [
            FilterRows(Condition("Extension", 'ne', 'MRIMGX'))
        ],
        context=ctx
    )

def normalize_categories(ctx: Context):
    return Pipeline(
        [
            Transform(ColProcessor(pd.DataFrame.fillna, value="Other"), NameFilter("category")),
        ],
        context=ctx
    )

def label_duplicates(ctx: Context):
    return Pipeline(
        [
            Compute(ColProcessor(pd.DataFrame.duplicated, keep=False), NameFilter("File:FileSize"), "IsSizeDuplicate"),
            Compute(ElementProcessor(calc_full_hash), NameFilter("SourceFile"), "FileHash", where=Condition("IsSizeDuplicate", "eq", True)),
            Compute(ColProcessor(pd.DataFrame.duplicated), NameFilter("FileHash"), "IsFileDuplicate", where=Condition("IsSizeDuplicate", "eq", True)),
            Transform(ColProcessor(pd.DataFrame.fillna, value=False), NameFilter("IsFileDuplicate")),
            Compute(ElementProcessor(label_duplicate), NameFilter("IsFileDuplicate"), "DuplicateLabel"),
        ],
        context=ctx
    )

def extract_year(ctx: Context):
    return Pipeline(
        [
            Transform(ElementProcessor(ctx.parser.parse), TagFilter(["created_dt", "modify_dt"])),
            Compute(RowProcessor(get_min_year), TagFilter(["created_dt", "modify_dt"]), "Year"),
        ],
        context=ctx
    )

def extract_country(ctx: Context):
    return Pipeline(
        [
            Compute(RowProcessor(get_country, geocoder=ctx.geocoder, lat_col="EXIF:GPSLatitude", lon_col="EXIF:GPSLongitude"), NameFilter(["EXIF:GPSLatitude", "EXIF:GPSLongitude"]), "Country", where=Condition('category', "eq", "Image")),
        ],
        context=ctx
    )

def extract_worksheets_count(ctx: Context):
    return Pipeline(
        [
            Compute(ElementProcessor(get_worksheets_count, target_headings=["Worksheets", "Листы"]), NameFilter("XML:HeadingPairs"), "CountWorksheets", where=Condition('category', "eq", "Data-Excel")),
        ],
        context=ctx
    )

def assemble_dest_dir(ctx: Context, dest_root: str, components: list[str]):
    return Pipeline(
        [
            Compute(RowProcessor(build_path, root=dest_root), NameFilter(components), dest_col="DestFileDir"),
        ],
        context=ctx
    )