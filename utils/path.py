import ctypes
import os
from typing import Iterable, Iterator
from tqdm import tqdm
from utils.text import strip_text, split_text, count_letters, count_char

# General
def get_abs_path(path: str) -> str:
    return os.path.abspath(path)

def get_common_path(paths: Iterable[str]) -> str:
    return os.path.commonpath(paths)

def get_normalized_path(path: str, path_separator: str = os.sep) -> str:
    normalized_path = strip_text(path, char_to_remove=path_separator)
    letters_count = count_letters(normalized_path)
    chars_count = len(normalized_path)
    if letters_count == 1 and chars_count == 2:
        return normalized_path + path_separator
    elif letters_count == 1 and chars_count == 1:
        return normalized_path + ":" + path_separator 
    return normalized_path

def get_path_length(path: str, path_separator: str = os.sep) -> int:
    path_elements = split_text(path, path_separator)
    path_length = len(path_elements)
    return path_length

# File specific
def is_file(path: str) -> bool:
    return os.path.isfile(path)

def is_not_file(path: str) -> bool:
    return not os.path.isfile(path)

def is_readonly(path: str) -> bool:
    FILE_ATTRIBUTE_READONLY = 0x1
    attrs = ctypes.windll.kernel32.GetFileAttributesW(path)
    if attrs == -1:
        return False
    return bool(attrs & FILE_ATTRIBUTE_READONLY)

def is_hidden(path: str) -> bool:
    FILE_ATTRIBUTE_HIDDEN = 0x2
    attrs = ctypes.windll.kernel32.GetFileAttributesW(path)
    if attrs == -1:
        return False
    return bool(attrs & FILE_ATTRIBUTE_HIDDEN)

def is_system(path: str) -> bool:
    FILE_ATTRIBUTE_SYSTEM = 0x4
    attrs = ctypes.windll.kernel32.GetFileAttributesW(path)
    if attrs == -1:
        return False
    return bool(attrs & FILE_ATTRIBUTE_SYSTEM)

def get_file_dir(path: str) -> str:
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")   
    return os.path.dirname(path)

def get_file_extension(path: str, ext_separator: str = '.') -> str:
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    basename = os.path.basename(path)
    if count_char(basename, ext_separator) > 0:
        return os.path.splitext(basename)[-1]
    else:
        return ""

def get_file_stem(path: str, ext_separator: str = '.') -> str:
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    basename = os.path.basename(path)
    if count_char(basename, ext_separator) > 0:
        return os.path.splitext(basename)[-2]
    else:
        return ""

def get_file_name(path):
    return os.path.basename(path)

def get_file_stat(path: str) -> dict:
    stat = os.stat(path)
    return {
        "UserId": stat.st_uid,
        "GroupId": stat.st_gid,
        "Name":get_file_name(path),
        "Ext": get_file_extension(path),
        "Size": stat.st_size,
        "isReadonly": is_readonly(path),
        "isHidden": is_hidden(path),
        "isSystem": is_system(path),
        "AccessedAt": stat.st_atime,
        "ModifiedAt": stat.st_mtime,
        "CreatedAt": stat.st_birthtime,
    }

# Dirs specific
def is_dir(path:str) -> bool:
    return os.path.isdir(path)

def is_not_dir(path:str) -> bool:
    return not os.path.isdir(path)

def is_parent(path: str, of_path: str) -> bool:
    if is_not_dir(path) or is_not_dir(of_path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    abs_path = get_abs_path(path)
    abs_of_path = get_abs_path(of_path)
    common_path = get_common_path([abs_path, abs_of_path])
    return abs_path == common_path and abs_path != abs_of_path

def get_root_dir(path: str) -> str:
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    abs_path = get_abs_path(path)
    drive_root, _ = os.path.splitdrive(abs_path)
    return drive_root

def get_dir_depth(path: str) -> int: # depth starting index 0 vs 1 ?
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    abs_path = get_abs_path(path)
    normalize_path = strip_text(abs_path, char_to_remove=os.sep)
    return get_path_length(normalize_path) - 1

def get_branch_depth(path: str) -> tuple[int, int]:
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    return max(
        get_dir_depth(root)
        for root, _ , _ in tqdm(os.walk(path), unit=" dir", desc=f"Scanning branch depth {path:<40}")
    )

def iter_dir_hierarchy(path: str, max_depth_from_root: int, pbar=None) -> Iterator[tuple[int, str]]:
    
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    
    for root, dirs, files in os.walk(path):
        current_depth = get_dir_depth(root)

        if pbar is not None:
            pbar.update(1)
            if not hasattr(pbar, "file_count"): pbar.file_count = 0
            pbar.file_count += len(files)
            pbar.set_postfix({"files": pbar.file_count}, refresh=False)

        if current_depth >= max_depth_from_root:
            dirs[:] = []
        yield current_depth, root, files

# def extesion_priority_sort(path: str, priority_map=None) -> tuple:
    
#     if is_not_file(path):
#         raise FileNotFoundError(f"No such file: {path}")
    
#     filename = get_file_basename(path) 
#     ext = get_file_extension(path)
#     dir = get_file_dir(path)

#     is_temp = 1 if filename.startswith("~") else 0
    
#     if priority_map is not None:
#         last_priority = max(priority_map.values()) + 1
#         priority = priority_map.get(ext, last_priority)
#         return (is_temp, priority, dir, filename)
    
#     return (is_temp, dir, filename)