class ColNames:
    ROOT = "Root"
    ROOT_PROCESSING_DEPTH = "RootProcessingDepth"
    FILE_NAME = "FileName"
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
    FILE_HASH = "FileHash"
    EXIF_ARGS = "ExifArgs"
    
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


def dest(name: str) -> str:
    return f"Dest{name}"
def dup(name: str) -> str:
    return f"{name}Dup"
def label(name: str) -> str:
    return f"{name}Label"
def add_prefix(name: str, prefix: str = '') -> str:
    return f"{prefix}{name}"