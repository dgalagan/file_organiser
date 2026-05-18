import pandas as pd
from core.exif_data import DateParser, get_earliest_date

created_tags = [
    # "exe:timestamp", # specific, actually holds date
    # "xmp:timestamp", # specific, actually holds date
    # "png:exifdatetime", # specific, actually holds date
    # "composite:gpsdatetime", # specific, actually holds date
    # "quicktime:purchasedate", # temporary, overlap with recognised receipt json 
    "createdate", # 18 instances
    "creationdate", # 7 instances
    "datetimeoriginal", # 8 instances
    "datetimedigitized", # 3 instances
    # "createddatetime", # 1 instance
    # "datetimecreated", # 1 instance
    # "encodingtime", # 2 instances
    # "profiledatetime", # 1 instance
    # "retaildate", # 2 instance
    # "ripdate", # 2 instance
    # "releasetime", # 2 instance
    # "originalreleaseyear", # 1 instance
]

dt_formats = {
    "dby_hm": "%d-%b-%Y %H:%M",                         # 27-Apr-2026 15:40, len 17
    "ymd_hm": "%Y-%m-%d %H:%M",                         # 2026-04-27 09:40, len 16
    "exif_ymd_hm": "%Y:%m:%d %H:%M",                    # 2026:04:27 09:40, len 16
    "exif_ymd_hm_tz": "%Y:%m:%d %H:%M%z",               # 2026-04-27 09:40+0200, len 21 or 22 
    "ymd_hms": "%Y-%m-%d %H:%M:%S",                     # 2026-04-29 13:40:05, len 19
    "exif_ymd_hms": "%Y:%m:%d %H:%M:%S",                # 2026:04:29 13:40:05, len 19
    "pg_ymd_hmsf": "%Y-%m-%d %H:%M:%S.%f",              # 2026-04-29 00:00:00.000000 len 20, 21, 22, 23, 24, 25, 26
    "exif_ymd_hmsf": "%Y:%m:%d %H:%M:%S.%f",            # 2026:04:29 00:00:00.000000 len 20, 21, 22, 23, 24, 25, 26 
    "exif_ymd_hmsf_tz": "%Y:%m:%d %H:%M:%S.%f%z",       # 2026:04:29 00:00:00.000000+0200 len 25, 26, 27, 28, 29, 30, 31 
    "8601_ymd_hms_utc": "%Y%m%dT%H%M%SZ",               # 20260427T154031Z len 16
    "8601_ymd_hms_tz": "%Y-%m-%dT%H:%M:%S%z",           # 2026-04-27T15:40:31+0200, len 24 or 25 
    "8601_ymd_hms": "%Y-%m-%dT%H:%M:%S",                # 2026-04-27T15:40:31, len 19
    "3339_ymd_hms_tz": "%Y-%m-%d %H:%M:%S%z",           # 2026-04-27 09:40:31+0200, len 24 or 25 
    "exif_ymd_hms_tz": "%Y:%m:%d %H:%M:%S%z",           # 2026:04:27 09:40:31+0200, len 24 or 25 
    "3339_ymd_hms_utc": "%Y-%m-%d %H:%M:%SZ",           # 2026-04-27 09:40:31Z, len 20
    "us_mdy_ims_p": "%m/%d/%Y %I:%M:%S %p",             # 04/27/2026 03:40:31 PM, len 22
    "us_mdy_im_p": "%m/%d/%Y %I:%M %p",                 # 04/29/2026 01:52 PM, len 19
    "eu_dmy_hms": "%d/%m/%Y %H:%M:%S",                  # 27/04/2026 15:40:31, len 19
    "eu_dmy_hm": "%d/%m/%Y %H:%M",                      # 29/04/2026 13:52, len 16
    "2822_adby_hms": "%a, %d %b %Y %H:%M:%S",           # Wed, 29 Apr 2026 00:00:00, len 25
    "2822_adby_hm": "%a, %d %b %Y %H:%M",               # Mon, 27 Apr 2026 09:40, len 22
    "mdy": "%m/%d/%y",                                  # 04/29/26, len 8
    "ymd": "%Y-%m-%d",                                  # 2026-04-29, len 10
    "exif_ymd": "%Y:%m:%d",                             # 2026:04:29, len 10
    "exif_ydm": "%Y:%d:%m",                             # 2026:29:04, len 10
    "bdy": "%B %d, %Y",                                 # April 29, 2026
    "dby": "%d %b %Y",                                  # 29 Apr 2026, len 11
    "yj": "%Y-%j",                                      # 2026-119, len 8
}

dt_patterns = {
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

dt_nulls = ["0000:00:00 00:00:00", "0000:01:01 00:00:00", "1980:00:00 00:00:00", "1980:01:01 00:00:00"]

exif_meta_df = pd.read_json("db\\exif_metadata.json", orient="records").set_index("SourceFile")
parser = DateParser(dt_patterns, dt_nulls)
dates_df = get_earliest_date(exif_meta_df, created_tags, parser)
dates_df.to_csv("output/dates.csv")
print(parser.dt_failed)
print(parser.unknown_patterns)
print(parser.summary)
