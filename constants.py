import os

# --- Project root ---
try:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
except NameError:
    PROJECT_ROOT = os.getcwd()

# --- Reference ---
REF_DIR = "ref"
REF_EXTENSION_MAP = "extension_map.json"
REF_DIR_PATH = os.path.join(PROJECT_ROOT, REF_DIR)
EXTENSION_MAP_PATH = os.path.join(REF_DIR_PATH, REF_EXTENSION_MAP)

# --- Cache ---
CACHE_DIR = "cache"
CACHE_METADATA = "metadata.json"
CACHE_REGISTER = "register.json"
CACHE_DIR_PATH = os.path.join(PROJECT_ROOT, CACHE_DIR)
METADATA_PATH = os.path.join(CACHE_DIR_PATH, CACHE_METADATA)
REGISTER_PATH = os.path.join(CACHE_DIR_PATH, CACHE_REGISTER)

# --- Output ---
OUTPUT_DIR = "output"
OUTPUT_DIR_PATH = os.path.join(PROJECT_ROOT, OUTPUT_DIR)

class Cols:

    ROOT = "Root"
    ROOT_INVALID = "RootInvalid"
    ROOT_DUP = "RootDup"
    ROOT_EMPTY = "RootEmpty"
    ROOT_SELECTED = "RootSelected"
    ROOT_DEPTH = "RootDepth"
    ROOT_TREE_DEPTH = "RootTreeDepth"
    ROOT_PROCESSING_DEPTH = "ProcessingDepth"
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

    @staticmethod
    def dest(name: str) -> str:
        return f"Dest{name}"
    @staticmethod
    def dup(name: str) -> str:
        return f"{name}Dup"
    @staticmethod
    def label(name: str) -> str:
        return f"{name}Label"
    @staticmethod
    def prefix(name: str, prefix: str) -> str:
        return f"{prefix}{name}"

class Tags:
    CREATE_DT = "create_dt"
    ACCESS_DT = "access_dt"
    MODIFY_DT = "modify_dt"
    COMPONENTS = "components"