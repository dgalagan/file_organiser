from functools import partial
import os
from tqdm import tqdm
from utils.path import iter_dir_hierarchy, extesion_priority_sort
import pandera as pa
from pandera.typing import Series
import pandas as pd
from typing import Optional

IMAGE_EXT = [".jpg", ".jpeg", ".png", ".heic", ".gif"]
VIDEO_EXT = [".avi", ".wmv", ".mkv", ".mov", ".mp4", ".mpeg", ".mpg"]
DATA_EXT = [".xls", ".xlsb", ".xlsm", ".xlsx", ".csv", ".ppt", ".pptm", ".pptx"]
DOCUMENTS_EXT = [".txt", ".md", ".doc", ".docx", ".rtf", ".pdf"]
PRIORITY_EXT = [IMAGE_EXT, VIDEO_EXT, DATA_EXT, DOCUMENTS_EXT]
PRIORITY_MAP = {ext: priority for priority, ext_list in enumerate(PRIORITY_EXT) for ext in ext_list}

# Schema
class FilesTrackerSchema(pa.DataFrameModel):
    FilePath: Series[str] = pa.Field(coerce=True)
    isFile: Optional[Series[bool]] = pa.Field(nullable=True)
    UserId: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    GroupId: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    Size: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    LastAccessTime: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    LastModifyDate: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    CreatedDate: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    Status: Optional[Series["Int64"]] = pa.Field(ge=0, nullable=True)
    
    class Config:
        strict = True
        coerce = True

def get_scope(input_dirs):
    dirs = []
    files = []
    for input_dir, max_depth in input_dirs:
        with tqdm(unit=" dir", desc=f"Extract files from {input_dir:<40}") as pbar:
            for depth, dir, dir_files in iter_dir_hierarchy(input_dir, max_depth, pbar=pbar):
                dirs.append(dir)
                file_path = [os.path.join(dir, dir_file) for dir_file in dir_files]
                files.extend(file_path)
    sort_with_map = partial(extesion_priority_sort, priority_map=PRIORITY_MAP)
    files.sort(key=sort_with_map)
    return dirs, files

