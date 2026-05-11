import datetime as dt
import json
import pandas as pd
import numpy as np
# df_1 = pd.read_json("db\\exif_metadata.json", orient="records").set_index("SourceFile")
# coverage = df_1.groupby(by="File:FileTypeExtension").apply(lambda group: group.notnull().mean() * 100)
# coverage.to_csv("coverage_1.csv")

# source: https://www.pythonmorsels.com/strptime/#formats
dt_formats = {
    "dby_hm": "%d-%b-%Y %H:%M",                         # 27-Apr-2026 15:40
    "ymd_hm": "%Y-%m-%d %H:%M",                         # 2026-04-27 09:40
    "ymd_hms": "%Y-%m-%d %H:%M:%S",                     # 2026-04-29 13:40:05
    "ymd_hms_e": "%Y:%m:%d %H:%M:%S",                   # 2026:04:29 13:40:05
    "ymd_hmsf_pg": "%Y-%m-%d %H:%M:%S.%f",              # 2026-04-29 00:00:00.000000
    "ymd_hms_utc_8601": "%Y%m%dT%H%M%SZ",               # 20260427T154031Z
    "ymd_hms_tz_8601": "%Y-%m-%dT%H:%M:%S%z",           # 2026-04-27T15:40:31+0200
    "ymd_hms_8601": "%Y-%m-%dT%H:%M:%S",                # 2026-04-27T15:40:31
    "ymd_hms_tz_3339": "%Y-%m-%d %H:%M:%S%z",           # 2026-04-27 09:40:31+0200
    "ymd_hms_tz_3339_e": "%Y:%m:%d %H:%M:%S%z",         # 2026:04:27 09:40:31+0200
    "ymd_hms_utc_3339": "%Y-%m-%d %H:%M:%SZ",           # 2026-04-27 09:40:31Z
    "ymd_hms_utc_3339_e": "%Y:%m:%d %H:%M:%SZ",         # 2026:04:27 09:40:31Z
    "mdy_ims_p_us": "%m/%d/%Y %I:%M:%S %p",               # 04/27/2026 03:40:31 PM
    "mdy_im_p_us": "%m/%d/%Y %I:%M %p",                   # 04/29/2026 01:52 PM
    "dmy_hms_eu": "%d/%m/%Y %H:%M:%S",                  # 27/04/2026 15:40:31
    "dmy_hm_eu": "%d/%m/%Y %H:%M",                      # 29/04/2026 13:52
    "adby_hms_2822": "%a, %d %b %Y %H:%M:%S",           # Wed, 29 Apr 2026 00:00:00
    "adby_hm_2822": "%a, %d %b %Y %H:%M",               # Mon, 27 Apr 2026 09:40
    "mdy": "%m/%d/%y",                                  # 04/29/26
    "ymd": "%Y-%m-%d",                                  # 2026-04-29
    "bdy": "%B %d, %Y",                                 # April 29, 2026
    "dby": "%d %b %Y",                                  # 29 Apr 2026
    "yj": "%Y-%j",                                      # 2026-119
}

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

access_tags = [
    "accessdate",
    "lastplayed",
    "lastprinted",
]

modify_tags = [
    "datemodify", # 1 instance
    "lastsaved", # 4 instance
    "lastupdated" # 0 instance
    "moddate", # 0 instance
    "modifydate", # 17 instance
    "metadatadate", # 2 instance
    "sourcemodified" # 2 instance
]

NULL_DATES = ["0000:00:00 00:00:00", "0000:01:01 00:00:00", "1980:00:00 00:00:00", "1980:01:01 00:00:00"]
DATETIME_SEPS = [' ', "T"]

def get_cols_by_keywords(cols, keywords=[]):
    
    target_cols = set()
    for keyword in keywords:
        for col in cols:
            if keyword in col.lower():
                target_cols.add(col)
    
    return list(target_cols)

def is_valid_date(date: str, null_dates=NULL_DATES):
    
    if isinstance(date, str):
        is_null = date in null_dates
        colon_count = date.count(":")
        dash_count = date.count("-")
        return not is_null and (colon_count + dash_count) in [4, 5]
    
    return False

# def datetime_to_timestamp(date_str, separators=DATETIME_SEPS):
    
#     separator = [sep for sep in separators if sep in date_str]
#     if separator:
#         date_elements = date_str.split(separator[0])
#         date_elements[0] = date_elements[0].replace(":", "-")
#         iso_date = separator[0].join(date_elements)
    
#     try:
#         timestamp = dt.datetime.fromisoformat(iso_date).timestamp()
#         return timestamp
#     except ValueError as ve:
#         print(f"{ve} - {date_str}")
#         return None


def datetime_to_timestamp(dt_str, dt_formats=["ymd_hms_tz_3339_e", "ymd_hms_utc_3339_e", "ymd_hms_e"]):
    if not isinstance(dt_str, str):
        # implement logging
        print(f"Ivalid data type for {dt_str}")
        return None
    
    for dt_format in dt_formats:
        format = dt_formats[dt_format]
        try:
            return dt.datetime.strptime(dt_str, format).timestamp()
        except Exception as e:
            # implement logging
            print(f"{e} for {dt_str}")
            continue
    return None

def get_earliest_date(df: pd.DataFrame, keywords=[]):
    
    # Select columns by keywords
    df_cols = df.columns
    selected_cols = get_cols_by_keywords(df_cols, keywords=keywords)
    dates_df = df[selected_cols]
    
    # Predicate cols
    timestamp_cols = []

    # Normalize dates
    for col in selected_cols:
        ts_col_name = f"{col}_ts"
        timestamp_cols.append(ts_col_name)
        dates_df[ts_col_name] = dates_df[col].apply(datetime_to_timestamp)
    
    # Calculate ealiest timestamp
    dates_df["EarliestTimestamp"] = dates_df[timestamp_cols].min(axis=1)
    dates_df["EarliestYear"] = dates_df["EarliestTimestamp"].apply(lambda x: dt.datetime.fromtimestamp(x).year)

    return dates_df
















# dates = {"colons_count": [], "field": [], "length":[], "original":[], "converted": [], "year": []}
# fails = []
# counter = 0
# for id, row in date_df.iterrows():
#     for i, item in row.items():
#         if not isinstance(item, int) and not isinstance(item, float):
#             if ':' in item:
#                 counter += 1
#                 try:
#                     timestamp = datetime_to_timestamp(item)
#                     # year = datetime.fromtimestamp(timestamp).year
#                     # dates["converted"].append(timestamp)
#                     # dates["year"].append(year)
#                 except Exception as e:
#                     fails.append((i, item))
#                 year = datetime.datetime.fromtimestamp(timestamp).year
#                 dates["field"].append(i)
#                 dates["colons_count"].append(item.count(":"))
#                 dates["length"].append(len(item))
#                 dates["original"].append(item)
#                 dates["converted"].append(timestamp)
#                 dates["year"].append(year)

# for key, value in dates.items():
#     print(key, len(value))

# print(fails)
# print(counter)
# df = pd.DataFrame(dates)
# df.to_csv("dates_2.csv")
 