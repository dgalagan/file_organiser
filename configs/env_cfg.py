import os
import platform

PLATFORM_SYS = platform.system()
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# File settings
ENCODING = "utf-8"

# JSON settings
JSON_SETTINGS = {
    "encoding": ENCODING,
    "indent": 4, 
    "ensure_ascii": False,
}

# File names
EXIF_DB_NAME = "exif_db.json"
HASH_DB_NAME = "hash_db.json"
EXTENSION_REF_NAME = "extension_ref.json"
EXIFTOOL_NAME = "exif"

# Build paths for required files
FILE_MANIFEST = {
    "ref": [EXTENSION_REF_NAME],
    "db": [EXIF_DB_NAME, HASH_DB_NAME]
}
FILE_PATHS = {}
for dir_name, required_file_names in FILE_MANIFEST.items():
    dir_path = os.path.join(PROJECT_DIR, dir_name)
    os.makedirs(dir_path, exist_ok=True)
    for required_file_name in required_file_names:
        required_file_path = os.path.join(dir_path, required_file_name)
        FILE_PATHS[required_file_name] = required_file_path

# Build paths for required executables
tool_executables = {
    EXIFTOOL_NAME: {
    "Windows": {"file": "exiftool(-k).exe", "url": 'https://sourceforge.net/projects/exiftool/files/exiftool-13.55_64.zip/download'},
    "Darwin": {"file": "exiftool", "url": 'https://sourceforge.net/projects/exiftool/files/Image-ExifTool-13.55.tar.gz/download'},
    }
}
EXECUTABLE_MANIFEST = {
    "bin": [EXIFTOOL_NAME],
}
EXECUTABLE_URLS = {}
EXECUTABLE_PATHS = {}
for dir_name, required_tool_names in EXECUTABLE_MANIFEST.items():
    dir_path = os.path.join(PROJECT_DIR, dir_name)
    os.makedirs(dir_path, exist_ok=True)
    for required_tool_name in required_tool_names:
        platforms_cfg = tool_executables[required_tool_name] # key error if absen
        platform_cfg = platforms_cfg[PLATFORM_SYS] # key error if not supported
        executable_file = platform_cfg.get("file")
        if not executable_file:
            raise RuntimeError(f"No executable file defined for '{required_tool_name}' for '{PLATFORM_SYS}'")
        executable_path = os.path.join(dir_path, required_tool_name, executable_file)
        EXECUTABLE_PATHS[required_tool_name] = executable_path
        EXECUTABLE_URLS[required_tool_name] = platform_cfg.get("url", '')