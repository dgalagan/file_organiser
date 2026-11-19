from core.col_names import ColNames, dest, add_suffix
from core.parser import DateParser
from dataframe.pipeline import Pipeline, Compute, Label, ExpandDict, CumCount
from dataframe.col_filter import NameFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor
from dataframe.predicate import Predicate, Condition, AllRows, And
import datetime as dt
import pandas as pd
from utils.path import parse_filename
from utils.strings import uppercase_text, lowercase_text
import os
import hashlib
from typing import Literal
from reverse_geocoder import RGeocoder

StatMetrics = Literal["st_size", "st_mtime", "st_dev", "st_ino"]

###############################
########### HELPERS ###########
###############################

def duplicated(df: pd.DataFrame, keep: Literal[False, "first", "last"]) -> pd.DataFrame:
    return df.duplicated(keep=keep)

def load_os_stat(path: str, metrics: list[str]) -> dict | None:
    try:
        stat = os.stat(path)
        return {metric: getattr(stat, metric) for metric in metrics}
    except (OSError, FileNotFoundError):
        # implement logging
        return None

def generate_file_id(row: pd.Series) -> str:
    dev = row.get(ColNames.INODE_DEV, row.get(dest(ColNames.INODE_DEV)))
    ino = row.get(ColNames.INODE, row.get(dest(ColNames.INODE)))
    return hashlib.md5(f"{dev}|{ino}".encode()).hexdigest()

def adjust_filename(row: pd.Series) -> str:
    filename, idx = row[ColNames.FILE_NAME], row[add_suffix(ColNames.FILE_NAME, "Index")]
    if not idx:
        return filename
    stem, ext = parse_filename(filename)
    return f"{stem}_{idx}.{ext}"

def build_file_path(row: pd.Series) -> str:
    dir_path = row.get(ColNames.FILE_DIR_PATH, row.get(dest(ColNames.FILE_DIR_PATH)))
    filename = row.get(ColNames.FILE_NAME, row.get(dest(ColNames.FILE_NAME)))
    return os.path.join(dir_path, filename)

def resolve_ext(row: pd.Series) -> str:
    stem, ext = parse_filename(row[ColNames.FILE_NAME])
    exif_ext = row[ColNames.FILE_TYPE_EXT]
    return uppercase_text(ext) if pd.isna(exif_ext) else exif_ext

def build_dir_path(row: pd.Series) -> str:
    return os.sep.join([str(value) for value in row.values if pd.notna(value)])

def get_country(row: pd.Series, geocoder: RGeocoder) -> str | None:
    lat, lon = row[ColNames.EXIF_GPS_LATITUDE], row[ColNames.EXIF_GPS_LONGITUDE]
    if pd.isna(lat) or pd.isna(lon):
        return None
    return geocoder.query([(lat, lon)])[0]["cc"]

def get_earliest_year(row: pd.Series) -> int:
    timestamp = row.min()
    if pd.isna(timestamp):
        timestamp = 0.0
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

def add_unique_filename(src_col: str, dest_col: str):
    '''
    Make file names unique across all rows, ignoring case.

    Steps:
        1. Lowercase the names, so "Word.csv" and "word.csv" compare equal.
        2. Number each row among rows with the same lowercased name, in row order. The first gets 0.
        3. Insert the number before the extension when it is not 0: "word.csv" -> "word_1.csv".
        4. (not implemented) Add a timestamp to avoid name clashes.
    '''

    # Type check
    if not isinstance(src_col, str):
        raise TypeError(f"Source column must be str, got {type(src_col)}")
    if not isinstance(dest_col, str):
        raise TypeError(f"Destination column must be str,  got {type(dest_col)}")

    _lowercase = add_suffix(src_col, "Lowercase")
    _index = add_suffix(src_col, "Index")

    # Semantic check
    if src_col != ColNames.FILE_NAME:
        raise ValueError(f"Got invalid column {src_col}")

    return Pipeline(
        [
            Compute(
                ElementProcessor(lowercase_text),
                NameFilter(src_col),
                dest_col=_lowercase
            ),
            CumCount(
                col=_lowercase,
                dest_col=_index
            ),
            Compute(
                RowProcessor(adjust_filename),
                NameFilter([src_col, _index]),
                dest_col=dest_col,
            )
        ]
    )

def add_file_path(src_cols: list[str], dest_col: str):
    '''
    Build a file path from a directory and file name col. 
    '''
    # Type check
    if not isinstance(src_cols, list):
        raise TypeError(f"Source columns must be list, got {type(src_cols)}")
    if not isinstance(dest_col, str):
        raise TypeError(f"Destination column must be str, got {type(dest_col)}")

    # Semantic check
    valid_pairs = ([ColNames.FILE_DIR_PATH, ColNames.FILE_NAME], [dest(ColNames.FILE_DIR_PATH), dest(ColNames.FILE_NAME)])
    if src_cols not in valid_pairs:
        raise ValueError(f"Got invalid columns to build file path {src_cols}")
    
    return Compute(
        RowProcessor(build_file_path),
        NameFilter(src_cols),
        dest_col=dest_col
    )

def add_os_stat(src_col: str, stat_to_dest_cols: dict[StatMetrics, str]):
    '''
    Write OS stat metric(s) of path(s) from src_col to the column(s) defined
    in stat_to_dest_col.
    '''
    # Type check
    if not isinstance(src_col, str):
        raise TypeError(f"Source column must be str, got {type(src_col)}")
    if not isinstance(stat_to_dest_cols, dict):
        raise TypeError(f"Metric to destination column must be dict, got {type(stat_to_dest_cols)}")
    
    os_stat_col = ColNames.OS_STAT
    
    return Pipeline(
        [
            Compute(
                ElementProcessor(load_os_stat, metrics=stat_to_dest_cols.keys()),
                NameFilter(src_col),
                dest_col=os_stat_col
            ),
            ExpandDict(
                col=os_stat_col,
                key_to_col=stat_to_dest_cols,
                where=Condition(src_col, "notna")
            )
        ]
    )

def add_file_id(src_cols: list[str], dest_col: str):
    return Compute(
        RowProcessor(generate_file_id),
        NameFilter(src_cols),
        dest_col=dest_col
    )

def consolidate_file_ext(src_cols: str | list[str], dest_col: str):
    return Compute(
        RowProcessor(resolve_ext),
        NameFilter(src_cols),
        dest_col=dest_col
    )


def add_duplicate_check(src_col: str, dest_col: str, cond: AllRows | Condition, keep: Literal["first", "last", False]):
    return Compute(
        ColProcessor(duplicated, keep=keep),
        NameFilter(src_col),
        dest_col=dest_col,
        where=And([cond, Condition(src_col, "notna")])
    )

def add_file_hash(src_col: str, dest_col: str, cond: AllRows | Condition):
    return Compute(
        ElementProcessor(calc_full_hash),
        NameFilter(src_col),
        dest_col=dest_col,
        where=cond
    )

def add_labels(dest_col: str, label_map: dict[str, Predicate]):
    return Label(
        dest_col=dest_col,
        label_to_cond=label_map
    )

def add_earliest_year(src_cols: list[str], dest_col: str, cond: AllRows | Condition, date_parser: DateParser):
    return Pipeline(
        [
            Compute(
                ElementProcessor(date_parser.parse),
                NameFilter(src_cols),
                where=cond 
            ),
            Compute(
                RowProcessor(get_earliest_year),
                NameFilter(src_cols),
                dest_col=dest_col,
                where=cond 
            )
        ]
    )

def add_image_country(src_cols: list[str], dest_col: str, cond: AllRows | Condition, geocoder: RGeocoder):
    return Compute(
        RowProcessor(get_country, geocoder=geocoder),
        NameFilter(src_cols),
        dest_col=dest_col,
        where=cond
    )

def add_n_worksheets(src_cols: list[str], dest_col: str, cond: AllRows | Condition):
    return Compute(
        ElementProcessor(get_worksheets_count, target_headings=["Worksheets", "Листы"]),
        NameFilter(src_cols), 
        dest_col=dest_col,
        where=cond
    )

def assemble_dest_dir(dir_dims: list[str], cond: AllRows | Condition):
    return Compute(
        RowProcessor(build_dir_path),
        col_filter=NameFilter(dir_dims),
        dest_col=dest(ColNames.FILE_DIR_PATH),
        where=cond
    )