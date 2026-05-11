import pandas as pd
from core.exif_data import datetime_to_timestamp, get_earliest_date

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

exif_meta_df = pd.read_json("db\\exif_metadata.json", orient="records").set_index("SourceFile")
dates_df = get_earliest_date(exif_meta_df, keywords=created_tags)
dates_df.to_csv("dates.csv")
print(dates_df.head())