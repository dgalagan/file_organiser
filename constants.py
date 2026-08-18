class Cols:

    SRC_ROOT = "SrcRoot"
    ROOT_INVALID = "RootInvalid"
    ROOT_DUP = "RootDup"
    ROOT_SELECTED = "RootSelected"
    ROOT_DEPTH = "RootDepth"
    ROOT_TREE_DEPTH = "RootTreeDepth"
    ROOT_PROCESSING_DEPTH = "ProcessingDepth"
    DIR_PATH = "DirPath"
    DIR_DEPTH = "DirDepth"
    FILE_NAME = "FileName"
    FILE_STEM = "FileStem"
    FILE_EXT = "FileExt"
    FILE_DIR_PATH = "FileDirPath"
    FILE_DIR_DEPTH = "FileDirDepth"
    FILE_PATH = "FilePath"
    FILE_STAT = "FileStat"
    SIZE = "Size"
    MODIFIED_AT = "ModifiedAt"
    INODE_DEV = "InodeDev"
    INODE = "Inode"
    FILE_ID = "CacheKey"
    EXIF_ARGS = "ExifArgs"
    FILE_HASH = "FileHash"

    # EXIF COLUMNS
    FILE_TYPE_EXT = "File:FileTypeExtension"
    EXIF_GPS_LATITUDE = "EXIF:GPSLatitude"
    EXIF_GPS_LONGITUDE = "EXIF:GPSLongitude"
    EXIF_MODEL = "EXIF:Model"
    XML_HEADING_PAIRS = "XML:HeadingPairs"
    ID3_YEAR = "ID3:Year"
    EXE_TIMESTAMP = "EXE:TimeStamp"
    XMP_TIMESTAMP = "XMP:Timestamp"
    PNG_DATETIME = "PNG:ExifDateTime"
    COMPOSITE_DATETIME = "Composite:GPSDateTime"
    QT_PURCHASE_DATE = "QuickTime:PurchaseDate"

    # CALC COLUMNS
    CONSOLIDATED_EXT = "ConsolidatedExt"
    FILE_CATEGORY = "FileCategory"
    IMAGE_COUNTRY = "ImageCountry"
    WORKSHEETS_COUNT = "WorksheetsCount"
    EARLIEST_YEAR = "EarliestYear"

class Tags:
    CREATE_DT = "create_dt"
    ACCESS_DT = "access_dt"
    MODIFY_DT = "modify_dt"

class TagsMapping:
    KEYWORD = {
        # "createddatetime", "datetimecreated", "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
        Tags.CREATE_DT: ["createdate", "creationdate", "datetimeoriginal", "datetimedigitized"],
        Tags.ACCESS_DT: ["accessdate", "lastplayed", "lastprinted"],
        Tags.MODIFY_DT: ["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"],
    }
    NAME = {
        Tags.CREATE_DT: [Cols.ID3_YEAR, Cols.EXE_TIMESTAMP, Cols.XMP_TIMESTAMP, Cols.PNG_DATETIME, Cols.COMPOSITE_DATETIME, Cols.QT_PURCHASE_DATE]
    }