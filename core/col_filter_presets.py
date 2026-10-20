from core.col_names import ColNames
from dataframe.col_filter import NameFilter, KeywordFilter, CombinedFilter

create_dt = CombinedFilter(
    [
        NameFilter([ColNames.ID3_YEAR, ColNames.EXE_TIMESTAMP, ColNames.XMP_TIMESTAMP, ColNames.PNG_DATETIME, ColNames.COMPOSITE_DATETIME, ColNames.QT_PURCHASE_DATE]),
        KeywordFilter(["createdate", "creationdate", "createddatetime", "datetimeoriginal", "datetimedigitized", "datetimecreated"]),
        # "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
    ]
)
access_dt = KeywordFilter(["accessdate", "lastplayed", "lastprinted"])
modify_dt = KeywordFilter(["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"])
date_cols = CombinedFilter([create_dt, access_dt, modify_dt])
registry_cols = NameFilter([ColNames.ROOT, ColNames.FILE_PATH, ColNames.MODIFIED_AT, ColNames.SIZE, ColNames.EXIF_ARGS])
metadata_cols = CombinedFilter(
    [
        date_cols,
        NameFilter([ColNames.FILE_TYPE_EXT, ColNames.EXIF_MODEL, ColNames.EXIF_GPS_LATITUDE, ColNames.EXIF_GPS_LONGITUDE, ColNames.XML_HEADING_PAIRS])
    ]
)
