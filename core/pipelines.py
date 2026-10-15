from core.col_names import ColNames, dest, dup, label, add_prefix
from core.parser import DateParser
from dataframe.pipeline import Pipeline, Compute, Label, ExpandDict
from dataframe.col_filter import ColumnFilter, NameFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor
from dataframe.predicate import Condition, AllRows
import datetime as dt
import pandas as pd
from utils.path import parse_filename
from utils.text import uppercase_text
import os
import hashlib
from typing import Literal
from reverse_geocoder import RGeocoder


###############################
########### HELPERS ###########
###############################

def safe_stat(file_path: str, metrics: list[str], prefix: str) -> os.stat_result | None:

    stat_cols = {
        "st_size": add_prefix(ColNames.SIZE, prefix),
        "st_mtime": add_prefix(ColNames.MODIFIED_AT, prefix),
        "st_dev": add_prefix(ColNames.INODE_DEV, prefix),
        "st_ino": add_prefix(ColNames.INODE, prefix),
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
    dir_path = next(row[col] for col in row.index if ColNames.FILE_DIR_PATH in col)
    filename = build_unique_filename(row[ColNames.FILE_NAME], row[ColNames.INODE]) if row.get(dup(ColNames.FILE_NAME), False) else row[ColNames.FILE_NAME]
    return os.path.join(dir_path, filename)

def build_dir_path(row: pd.Series) -> str:
    return os.sep.join([str(value) for value in row.values if pd.notna(value)])

def resolve_ext(row: pd.Series) -> str:
    _, ext = parse_filename(row[ColNames.FILE_NAME])
    exif_ext = row[ColNames.FILE_TYPE_EXT]
    return uppercase_text(ext) if pd.isna(exif_ext) else exif_ext

def get_id(row: pd.Series, prefix: str) -> str:
    dev = row[add_prefix(ColNames.INODE_DEV, prefix)]
    ino = row[add_prefix(ColNames.INODE, prefix)]
    return hashlib.md5(f"{dev}|{ino}".encode()).hexdigest()

def get_country(row: pd.Series, geocoder: RGeocoder) -> str | None:
    lat = row[ColNames.EXIF_GPS_LATITUDE]
    lon = row[ColNames.EXIF_GPS_LONGITUDE]
    
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

def assemble_file_path(prefix: Literal['', "Dest"]):
    if prefix:
        return Pipeline(
            [
                Compute(
                    ColProcessor(duplicated_ci, keep="first"), NameFilter(ColNames.FILE_NAME),
                    dest_col=dup(ColNames.FILE_NAME)
                ),
                Compute(
                    RowProcessor(build_file_path), NameFilter([add_prefix(ColNames.FILE_DIR_PATH, prefix), ColNames.FILE_NAME, dup(ColNames.FILE_NAME), ColNames.INODE]),
                    dest_col=add_prefix(ColNames.FILE_PATH, prefix)
                ),
            ]
        )
    
    return Pipeline(
        [
            Compute(
                RowProcessor(build_file_path), NameFilter([add_prefix(ColNames.FILE_DIR_PATH, prefix), ColNames.FILE_NAME]),
                dest_col=add_prefix(ColNames.FILE_PATH, prefix)
            )
        ]
    )

def add_stat(prefix: Literal['', "Dest"], metrics: Literal["st_size", "st_mtime", "st_dev", "st_ino"]):
    return Pipeline(
        [
            Compute(
                ElementProcessor(safe_stat, metrics=metrics, prefix=prefix), NameFilter(add_prefix(ColNames.FILE_PATH, prefix)),
                dest_col=add_prefix(ColNames.FILE_STAT, prefix)
            ),
            ExpandDict(
                col=add_prefix(ColNames.FILE_STAT, prefix),
                where=Condition(add_prefix(ColNames.FILE_STAT, prefix), "notna")
            )
        ]
    )

def add_file_id(prefix: Literal['', "Dest"]):
    return Compute(
        RowProcessor(get_id, prefix=prefix), NameFilter([add_prefix(ColNames.INODE_DEV, prefix), add_prefix(ColNames.INODE, prefix)]),
        dest_col=add_prefix(ColNames.FILE_ID, prefix)
    )

def consolidate_file_ext():
    return Compute(
        RowProcessor(resolve_ext), NameFilter([ColNames.FILE_TYPE_EXT, ColNames.FILE_NAME]),
        dest_col=ColNames.CONSOLIDATED_EXT
    )

def build_dims_map(geocoder: RGeocoder, date_parser: DateParser, date_col_filter: ColumnFilter) -> dict[str, Pipeline | Compute]:
    return {
        label(dup(ColNames.FILE_HASH)): Pipeline(
            [
                Compute(
                    ColProcessor(duplicated, keep=False),
                    col_filter=NameFilter(ColNames.SIZE),
                    dest_col=dup(ColNames.SIZE),
                    where=Condition(ColNames.SIZE, "notna")
                ),
                Compute(
                    ElementProcessor(calc_full_hash),
                    col_filter=NameFilter(ColNames.FILE_PATH),
                    dest_col=ColNames.FILE_HASH,
                    where=Condition(dup(ColNames.SIZE), "eq", True)
                ),
                Compute(
                    ColProcessor(duplicated, keep="first"),
                    col_filter=NameFilter(ColNames.FILE_HASH),
                    dest_col=dup(ColNames.FILE_HASH),
                    where=Condition(dup(ColNames.SIZE), "eq", True)
                ),
                Label(
                    dest_col=label(dup(ColNames.FILE_HASH)),
                    value="dup",
                    where=Condition(dup(ColNames.FILE_HASH), "eq", True)
                ),
                Label(
                    dest_col=label(dup(ColNames.FILE_HASH)),
                    value="unique",
                    where=Condition(dup(ColNames.FILE_HASH), "ne", True)
                )
            ]
        ),
        ColNames.EARLIEST_YEAR: Pipeline(
            [
                Compute(
                    ElementProcessor(date_parser.parse),
                    col_filter=date_col_filter
                ),
                Compute(
                    RowProcessor(get_earliest_year),
                    col_filter=date_col_filter,
                    dest_col=ColNames.EARLIEST_YEAR
                )
            ]
        ),
        ColNames.IMAGE_COUNTRY: Compute(
            RowProcessor(get_country, geocoder=geocoder),
            col_filter=NameFilter([ColNames.EXIF_GPS_LATITUDE, ColNames.EXIF_GPS_LONGITUDE]),
            dest_col=ColNames.IMAGE_COUNTRY,
            where=Condition(ColNames.FILE_CATEGORY, "eq", "Image")
        ),
        ColNames.WORKSHEETS_COUNT: Compute(
            ElementProcessor(get_worksheets_count, target_headings=["Worksheets", "Листы"]),
            col_filter=NameFilter(ColNames.XML_HEADING_PAIRS), 
            dest_col=ColNames.WORKSHEETS_COUNT,
            where=Condition(ColNames.FILE_CATEGORY, "eq", "Data-Excel")
        )
    }

def assemble_dest_dir(dir_dims: list[str], file_category: str):
    return Compute(
        RowProcessor(build_dir_path),
        col_filter=NameFilter(dir_dims),
        dest_col=dest(ColNames.FILE_DIR_PATH),
        where=Condition(ColNames.FILE_CATEGORY, "eq", file_category) if file_category else AllRows()
    )