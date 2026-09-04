from core.parser import DateParser
from dataframe.pipeline import Pipeline, FilterRows, Compute, Label, ExpandDict
from dataframe.col_filter import NameFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor
from dataframe.predicate import Predicate, Condition, And, Or, AllRows
from core.tagstore import TagStore
import datetime as dt
import pandas as pd
from utils.path import is_not_dir, is_empty, get_normalized_path, depth_from_drive, tree_depth, parse_filename
from utils.text import uppercase_text
import os
import hashlib
from constants import Cols, Tags
from typing import Literal
from reverse_geocoder import RGeocoder

###############################
########### HELPERS ###########
###############################

def safe_stat(file_path: str, metrics: list[str], prefix: str) -> os.stat_result | None:

    stat_cols = {
        "st_size": Cols.prefix(Cols.SIZE, prefix),
        "st_mtime": Cols.prefix(Cols.MODIFIED_AT, prefix),
        "st_dev": Cols.prefix(Cols.INODE_DEV, prefix),
        "st_ino": Cols.prefix(Cols.INODE, prefix),
    }

    if os.path.isfile(file_path):
        try:
            stat = os.stat(file_path)
            return {stat_cols[metric]: getattr(stat, metric) for metric in metrics}
        except OSError:
            return None

def duplicated_ci(df: pd.DataFrame, keep: Literal[False, "first", "last"]) -> pd.DataFrame:
    return df.iloc[:, 0].str.lower().duplicated(keep=keep)
def duplicated(df: pd.DataFrame, keep: Literal[False, "first", "last"]) -> pd.DataFrame:
    return df.duplicated(keep=keep)

def build_unique_filename(filename: str, ino: int) -> str:
    stem, ext = parse_filename(filename)
    return f"{stem}_{str(ino)}.{ext}"

def build_file_path(row: pd.Series) -> str:
    dir_path = next(row[col] for col in row.index if Cols.FILE_DIR_PATH in col)
    filename = build_unique_filename(row[Cols.FILE_NAME], row[Cols.INODE]) if row.get(Cols.dup(Cols.FILE_NAME), False) else row[Cols.FILE_NAME]
    return os.path.join(dir_path, filename)

def build_dir_path(row: pd.Series, root: str, dims: list[str] = None) -> str:
    if dims:
        parts = [str(row[dim]) for dim in dims if pd.notna(row[dim])]
        return os.path.join(root, *parts)
    return root

def resolve_ext(row: pd.Series) -> str:
    _, ext = parse_filename(row[Cols.FILE_NAME])
    exif_ext = row[Cols.FILE_TYPE_EXT]
    return uppercase_text(ext) if pd.isna(exif_ext) else exif_ext

def get_id(row: pd.Series, prefix: str) -> str:
    dev = row[Cols.prefix(Cols.INODE_DEV, prefix)]
    ino = row[Cols.prefix(Cols.INODE, prefix)]
    return hashlib.md5(f"{dev}|{ino}".encode()).hexdigest()

def get_country(row: pd.Series, geocoder: RGeocoder) -> str:
    lat = row[Cols.EXIF_GPS_LATITUDE]
    lon = row[Cols.EXIF_GPS_LONGITUDE]
    
    if pd.isna(lat) or pd.isna(lon):
        return None

    return geocoder.query([(lat, lon)])[0]["cc"]

def get_earliest_year(row: pd.Series) -> int:
    timestamp = row.min()
    if pd.isna(timestamp):
        return dt.datetime.fromtimestamp(0.0).year
    return dt.datetime.fromtimestamp(timestamp).year

def get_worksheets_count(heading_pairs: list, target_headings: list[str] = []) -> int:
    
    if not isinstance(heading_pairs, list):
        return None

    for i, heading in enumerate(heading_pairs):
        if heading in target_headings and i + 1 < len(heading_pairs):
            return heading_pairs[i + 1]
        return 0

# def calc_partial_hash(path: str, hash_algo: str, parts: int, read_cap: int) -> dict:
#     hash_func = getattr(hashlib, hash_algo)
#     file_size = os.path.getsize(path)
#     file_parts = file_size // parts
#     # remainder = file_size % parts
#     byte_steps = [file_parts * step for step in range(parts)]
#     combined_hash = hash_func()
#     try:
#         with open(path, "rb") as f:
#             for byte_step in byte_steps:
#                 f.seek(byte_step, 0)
#                 data = f.read(read_cap)
#                 combined_hash.update(data)
#         return {"hash": combined_hash.hexdigest()}
#     except PermissionError:
#         return {"hash": ""}
    
def calc_full_hash(path: str, hash_algo: str = "md5", buf_size: int = 65536) -> str:
    try:
        # hashlib.algorithms_available
        hash_func = hashlib.new(hash_algo)
        with open(path, "rb") as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    break
                hash_func.update(data)
        return hash_func.hexdigest()
    except PermissionError:
        return ""

###############################
#### DF PIPELINE FUNCTIONS ####
###############################

### SRC ROOT

def validate_dirs():
    return Pipeline(
        [
            Compute(ElementProcessor(get_normalized_path), NameFilter(Cols.ROOT)),
            Compute(ElementProcessor(is_not_dir), NameFilter(Cols.ROOT), dest_col=Cols.ROOT_INVALID),
            Compute(ElementProcessor(is_empty), NameFilter(Cols.ROOT), dest_col=Cols.ROOT_EMPTY, where=Condition(Cols.ROOT_INVALID, "eq", False)),
            Compute(ColProcessor(duplicated, keep="first"), NameFilter(Cols.ROOT), dest_col=Cols.dup(Cols.ROOT)),
        ]
    )

def add_depth_metrics():
    return Pipeline(
        [
            Compute(ElementProcessor(depth_from_drive), NameFilter(Cols.ROOT), dest_col=Cols.ROOT_DEPTH),
            Compute(ElementProcessor(tree_depth), NameFilter(Cols.ROOT), dest_col=Cols.ROOT_TREE_DEPTH),
        ]
    )

### FILES

def assemble_file_path(prefix: Literal["", "Dest"], tagstore: TagStore = None):

    file_dir_path = Cols.dest(Cols.FILE_DIR_PATH) if prefix else Cols.FILE_DIR_PATH
    file_path = Cols.dest(Cols.FILE_PATH) if prefix else Cols.FILE_PATH

    if prefix:
        return Pipeline(
            [
                Compute(
                    ColProcessor(duplicated_ci, keep="first"), NameFilter(Cols.FILE_NAME),
                    dest_col=Cols.dup(Cols.FILE_NAME),
                    tagstore=tagstore
                ),
                Compute(
                    RowProcessor(build_file_path), NameFilter([file_dir_path, Cols.FILE_NAME, Cols.dup(Cols.FILE_NAME), Cols.INODE]),
                    dest_col=file_path,
                    tagstore=tagstore
                ),
            ]
        )
    return Pipeline(
        [
            Compute(
                RowProcessor(build_file_path), NameFilter([file_dir_path, Cols.FILE_NAME]),
                dest_col=file_path,
                tagstore=tagstore
            )
        ]
    )

def add_stat(prefix: Literal["", "Dest"], metrics: Literal["st_size", "st_mtime", "st_dev", "st_ino"], tagstore: TagStore = None):
    return Pipeline(
        [
            Compute(
                ElementProcessor(safe_stat, metrics=metrics, prefix=prefix), NameFilter(Cols.prefix(Cols.FILE_PATH, prefix)),
                dest_col=Cols.prefix(Cols.FILE_STAT, prefix),
                tagstore=tagstore
            ),
            ExpandDict(
                col=Cols.prefix(Cols.FILE_STAT, prefix),
                where=Condition(Cols.prefix(Cols.FILE_STAT, prefix), "notna")
            )
        ]
    )

def add_file_id(prefix: Literal["", "Dest"], tagstore: TagStore = None):
    return Compute(
        RowProcessor(get_id, prefix=prefix), NameFilter([Cols.prefix(Cols.INODE_DEV, prefix), Cols.prefix(Cols.INODE, prefix)]),
        dest_col=Cols.prefix(Cols.FILE_ID, prefix),
        tagstore=tagstore
    )

def consolidate_file_ext(tagstore: TagStore = None):
    return Compute(
        RowProcessor(resolve_ext), NameFilter([Cols.FILE_TYPE_EXT, Cols.FILE_NAME]),
        dest_col=Cols.CONSOLIDATED_EXT,
        tagstore=tagstore
    )

def prepare_dimensions_calc(geocoder: RGeocoder, date_cols: list[str], date_parser: DateParser, tagstore: TagStore = None):
    return {
        Cols.label(Cols.dup(Cols.FILE_HASH)): Pipeline(
            [
                Compute(
                    ColProcessor(duplicated, keep=False), NameFilter(Cols.SIZE),
                    dest_col=Cols.dup(Cols.SIZE),
                    where=Condition(Cols.SIZE, "notna"),
                    tagstore=tagstore
                ),
                Compute(
                    ElementProcessor(calc_full_hash), NameFilter(Cols.FILE_PATH),
                    dest_col=Cols.FILE_HASH,
                    where=Condition(Cols.dup(Cols.SIZE), "eq", True),
                    tagstore=tagstore
                ),
                Compute(
                    ColProcessor(duplicated, keep="first"), NameFilter(Cols.FILE_HASH),
                    dest_col=Cols.dup(Cols.FILE_HASH),
                    where=Condition(Cols.dup(Cols.SIZE), "eq", True),
                    tagstore=tagstore
                ),
                Label(
                    dest_col=Cols.label(Cols.dup(Cols.FILE_HASH)),
                    value="dup",
                    where=Condition(Cols.dup(Cols.FILE_HASH), "eq", True)
                ),
                Label(
                    dest_col=Cols.label(Cols.dup(Cols.FILE_HASH)),
                    value="unique",
                    where=Condition(Cols.dup(Cols.FILE_HASH), "ne", True)
                )
            ]
        ),
        Cols.EARLIEST_YEAR: Pipeline(
            [
                Compute(
                    ElementProcessor(date_parser.parse), NameFilter(date_cols),
                    tagstore=tagstore
                ),
                Compute(
                    RowProcessor(get_earliest_year), NameFilter(date_cols),
                    dest_col=Cols.EARLIEST_YEAR,
                    tagstore=tagstore
                )
            ]
        ),
        Cols.IMAGE_COUNTRY: Compute(
            RowProcessor(get_country, geocoder=geocoder), NameFilter([Cols.EXIF_GPS_LATITUDE, Cols.EXIF_GPS_LONGITUDE]),
            dest_col=Cols.IMAGE_COUNTRY,
            where=Condition(Cols.FILE_CATEGORY, "eq", "Image"),
            tagstore=tagstore
        ),
        Cols.WORKSHEETS_COUNT: Compute(
            ElementProcessor(get_worksheets_count, target_headings=["Worksheets", "Листы"]), NameFilter(Cols.XML_HEADING_PAIRS), 
            dest_col=Cols.WORKSHEETS_COUNT,
            where=Condition(Cols.FILE_CATEGORY, "eq", "Data-Excel"),
            tagstore=tagstore
        )
    }

def assemble_dest_dir(dest_root: str, file_category: str = None, dims: list[str] = None, tagstore: TagStore = None):
    return Compute(
        RowProcessor(build_dir_path, root=dest_root, dims=dims),
        dest_col=Cols.dest(Cols.FILE_DIR_PATH),
        where=Condition(Cols.FILE_CATEGORY, "eq", file_category) if file_category else AllRows(),
        tagstore=tagstore
    )