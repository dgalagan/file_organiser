from exiftool import ExifTool
import json
import os
import shutil
from typing import Iterator

def find_exiftool() -> str:
    path = os.environ.get("EXIFTOOL_PATH") or shutil.which("exiftool")
    if not path:
        raise RuntimeError("ExifTool not found")
    return path

def get_batches(files: list[str], batch_size: int) -> list[list[str]]:
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

def extract_exif_data(exif_path: str, files: list[str], args: list[str], batch_size: int = 0) -> Iterator[dict]:
    batch_size = batch_size if batch_size > 0 else len(files)
    with ExifTool(encoding="utf-8", executable=exif_path) as et:
        for batch in get_batches(files, batch_size=batch_size):
            raw_output = et.execute(*args, *batch)
            yield from json.loads(raw_output)


### AUTO DOWNLOAD FUNCTIONALITY
# import platform
# import os
# import urllib.request
# import io
# import zipfile
# import tarfile
# import shutil

# PLATFORM_SYS = platform.system()
# PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# # File names
# EXIFTOOL_NAME = "exif"

# # Build paths for required executables
# tool_executables = {
#     EXIFTOOL_NAME: {
#     "Windows": {"file": "exiftool(-k).exe", "url": 'https://sourceforge.net/projects/exiftool/files/exiftool-13.55_64.zip/download'},
#     "Darwin": {"file": "exiftool", "url": 'https://sourceforge.net/projects/exiftool/files/Image-ExifTool-13.55.tar.gz/download'},
#     }
# }

# def download_tool(url: str, dest_dir: str) -> None:
    
#     print(f"Downloading from {url}...")
#     with urllib.request.urlopen(url) as response:
#         data = io.BytesIO(response.read())
#         final_url = response.url
    
#     print(f"Extracting {final_url}...")
#     if ".zip" in final_url:
#         with zipfile.ZipFile(data) as z:
#             root = z.namelist()[0].split("/")[0] 
#             z.extractall(dest_dir)
#         nested = os.path.join(dest_dir, root)
#         for item in os.listdir(nested):
#             shutil.move(os.path.join(nested, item), os.path.join(dest_dir, item))
#         os.rmdir(nested)
#     elif ".tar.gz" in final_url or ".tgz" in final_url:
#         with tarfile.open(fileobj=data, mode="r:gz") as t:
#             t.extractall(dest_dir)
#     else:
#         raise RuntimeError(f"Unsupported archive format: {url}")