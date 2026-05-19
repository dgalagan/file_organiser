import json
import datetime as dt
from utils.text import get_chars_pattern

class DateParser:
    def __init__(self):
        self.dt_patterns = {
            "ddddsddsdd":                       "%Y{s0}%m{s1}%d",
            "ddddsddsddwddsdd":                 "%Y{s0}%m{s1}%d %H{s2}%M",
            "ddddsddsddwddsddl":                "%Y{s0}%m{s1}%d %H{s2}%MZ",
            "ddddsddsddwddsddsdd":              "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S",
            "ddddsddsddlddsddsddl":             "%Y{s0}%m{s1}%dT%H{s2}%M{s3}%SZ",
            "ddddsddsddwddsddsddl":             "%Y{s0}%m{s1}%d %H{s2}%M{s3}%SZ",
            "ddddsddsddwddsddsddsdd":           "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
            "ddddsddsddwddsddsddsddd":          "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f",
            "ddddsddsddwddsddsddsddsdd":        "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S%z",
            "ddddsddsddwddsddsddsdddsddsdd":    "%Y{s0}%m{s1}%d %H{s2}%M{s3}%S{s4}%f%z"
        }
        self.dt_nulls = [
            "0000:00:00 00:00:00",
            "0000:01:01 00:00:00",
            "1980:00:00 00:00:00",
            "1980:01:01 00:00:00"
        ]
        self.dt_failed = []
        self.unknown_patterns = set()
        self.summary = {dt_pattern: [0, 0] for dt_pattern in self.dt_patterns}

    def parse(self, dt_str: str):
        # Ensure dt is not null
        if dt_str in self.dt_nulls:
            return None
        
        # Extract chars pattern
        chars_pattern, sep_args = get_chars_pattern(dt_str)
        
        if chars_pattern not in self.dt_patterns:
            self.unknown_patterns.add(chars_pattern)
            return None
        
        # Convert dt into timestamp
        try:
            dt_strf = self.dt_patterns[chars_pattern].format(**sep_args)
            timestamp = dt.datetime.strptime(dt_str, dt_strf).timestamp()
            self.summary[chars_pattern][0] += 1 
            return timestamp
        except Exception as e:
            self.dt_failed.append(dt_str)
            self.summary[chars_pattern][1] += 1 
            return None

def get_timestamp(date: str):
    if not isinstance(date, str):
        return None
    return DateParser().parse(date)

def get_year(timestamp: float) -> int:
    return dt.datetime.fromtimestamp(timestamp).year

def get_worksheets_count(heading_pairs: list) -> int:
    if not isinstance(heading_pairs, list):
        return None
    
    target_headings = ["Worksheets", "Листы"]

    for i, heading in enumerate(heading_pairs):
        if heading in target_headings and i + 1 < len(heading_pairs):
            return heading_pairs[i + 1]

    return None

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