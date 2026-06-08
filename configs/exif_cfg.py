import os
from configs.env_cfg import PROJECT_DIR, PLATFORM_SYS
from utils.text import lowercase_text

#########          EXECUTABLE PATH          #########
bin_dir = "bin"
exif_dir = "exiftool"
if PLATFORM_SYS == "Windows":
    exif_file_name = "exiftool.exe"
    EXIF_PATH = os.path.join(PROJECT_DIR, bin_dir, lowercase_text(PLATFORM_SYS), exif_dir, exif_file_name)
elif PLATFORM_SYS == "Darwin":
    exif_file_name = "exiftool"
    EXIF_PATH = os.path.join(PROJECT_DIR, bin_dir, lowercase_text(PLATFORM_SYS), exif_dir, exif_file_name)
else:
    raise NotImplementedError(f"Operating system '{PLATFORM_SYS}' is not supported.")

#########       EXTRACTION PARAMETERS       #########
exif_params = ["-j", "-G"]
include_tags = ["-all"]
exclude_tags = ["--File:Directory"]
exif_args = [*exif_params, *include_tags, *exclude_tags]
batch_size = 0

#########           CONFIGURATION           #########
EXIF_CFG = {
    "args": exif_args,
    "batch_size": batch_size,
    "exifpath": EXIF_PATH
}