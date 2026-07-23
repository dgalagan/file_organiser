import datetime as dt
import pandas as pd
import os
from reverse_geocoder import RGeocoder
from utils.text import get_chars_pattern
import hashlib

class DateParser:
    def __init__(self):
        self.dt_patterns = {
            "dddd":                             "%Y",
            "ddddsddsdd":                       "%Y{s0}%m{s1}%d",
            "ddddsddsddwddsdd":                 "%Y{s0}%m{s1}%d %H{s2}%M",
            "ddddsddsddwddsddl":                "%Y{s0}%m{s1}%d %H{s2}%MZ",
            "ddddsddsddwddsddsdd":              "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S",
            "ddddsddsddlddsddsdd":              "%Y{s0}%m{s1}%dT%H{s2}%M{s3}%S",
            "ddddsddsddlddsddsddl":             "%Y{s0}%m{s1}%dT%H{s2}%M{s3}%SZ",
            "ddddsddsddwddsddsddl":             "%Y{s0}%m{s1}%d %H{s2}%M{s3}%SZ",
            "ddddsddsddlddsddsddsddsdd":        "%Y{s0}%m{s1}%dT%H{s2}%M{s3}%S%z",
            "ddddsddsddwddsddsddsddsdd":        "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S%z",
            
            "ddddsddsddwddsddsddsd":            "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
            "ddddsddsddwddsddsddsdd":           "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
            "ddddsddsddwddsddsddsddd":          "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
            "ddddsddsddwddsddsddsdddd":         "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
            "ddddsddsddwddsddsddsddddd":        "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
            "ddddsddsddwddsddsddsdddddd":       "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",

            "ddddsddsddwddsddsddsdsddsdd":      "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z",
            "ddddsddsddwddsddsddsddsddsdd":     "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z",
            "ddddsddsddwddsddsddsdddsddsdd":    "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z",
            "ddddsddsddwddsddsddsddddsddsdd":   "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z",
            "ddddsddsddwddsddsddsdddddsddsdd":  "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z",
            "ddddsddsddwddsddsddsddddddsddsdd": "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z",
        }
        self.dt_nulls = ["0000:00:00 00:00:00", "0000:01:01 00:00:00", "1980:00:00 00:00:00", "1980:01:01 00:00:00"]
        self.summary = {dt_pattern: {"success": 0, "failed": 0, "null": 0, "failed_dates":[]} for dt_pattern in self.dt_patterns}

    def parse(self, date):
        # Do not process nan values
        if pd.isna(date):
            return None
        
        date_str = str(date) if not isinstance(date, str) else date

        # Parse the string into its structural character pattern and separators
        chars_pattern, sep_args = get_chars_pattern(date_str)

        # Log unvalidated patterns and skip processing if the format is unrecognized
        if chars_pattern not in self.dt_patterns:
            self.summary[chars_pattern] = {"success": 0, "failed": 0, "null": 0, "failed_dates":[]}

        # Reject known null-equivalent or placeholder strings
        if date_str in self.dt_nulls:
            self.summary[chars_pattern]["null"] += 1
            return None

        # Dynamically build the datetime format string and parse it into a Unix timestamp
        try:
            dt_strf = self.dt_patterns[chars_pattern].format(**sep_args)
            timestamp = dt.datetime.strptime(date_str, dt_strf).timestamp()
            self.summary[chars_pattern]["success"] += 1
            return timestamp
        except:
            self.summary[chars_pattern]["failed_dates"].append(date_str)
            self.summary[chars_pattern]["failed"] += 1
            return None
    
    def get_summary(self):
        return self.summary

def label_duplicate(value: bool) -> str:
    return "duplicate" if value else "original"

def get_worksheets_count(heading_pairs: list, target_headings: list[str] = []) -> int:
    
    if not isinstance(heading_pairs, list):
        return None

    for i, heading in enumerate(heading_pairs):
        if heading in target_headings and i + 1 < len(heading_pairs):
            return heading_pairs[i + 1]

    return None

def get_country(row: pd.Series, geocoder: RGeocoder, lat_col: str = "", lon_col: str = "") -> str:
    lat, lon = row.get(lat_col), row.get(lon_col)
    
    if pd.isna(lat) or pd.isna(lon):
        return None

    return geocoder.query([(lat, lon)])[0]["cc"]

def get_min_year(row: pd.Series) -> int:
    timestamp = row.min()
    return dt.datetime.fromtimestamp(timestamp).year

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
    
def build_path(row: pd.Series, dest_dir: str) -> pd.Series:
    path_fragments = [str(value) for value in row if pd.notna(value)]
    return os.path.join(dest_dir, *path_fragments)

def fill_na_from_col(row: pd.Series, from_col: str = "", to_col: str = ""):
    if pd.isna(row[to_col]):
        row[to_col] = row[from_col]
    return row