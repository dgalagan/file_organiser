import datetime as dt
import pandas as pd
from utils.text import get_chars_pattern

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