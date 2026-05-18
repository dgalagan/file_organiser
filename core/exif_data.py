import datetime as dt
from utils.text import get_chars_pattern

class DateParser:
    def __init__(self, dt_patterns, nulls):
        self.dt_patterns = dt_patterns
        self.dt_nulls = nulls
        self.dt_failed = []
        self.unknown_patterns = set()
        self.summary = {dt_pattern: [0, 0] for dt_pattern in dt_patterns}

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

def get_worksheets_count(heading_pairs: list) -> int:
    if not isinstance(heading_pairs, list):
        return None
    
    target_headings = ["Worksheets", "Листы"]

    for i, heading in enumerate(heading_pairs):
        if heading in target_headings and i + 1 < len(heading_pairs):
            return heading_pairs[i + 1]

    return None