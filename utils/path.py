import os
from typing import Iterable, Iterator
from utils.text import strip_text, split_text, count_letters, find_char
from tqdm import tqdm

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

def get_file_extension(path: str, ext_separator: str = '.') -> str:
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    if find_char(path, ext_separator) > 0:
        return os.path.splitext(path)[1]
    else:
        return os.path.splitext(path)[0]

def get_file_basename(path: str, ext_separator: str = '.') -> str: # not used
    if is_not_file(path):
        raise FileNotFoundError(f"No such file: {path}")
    if find_char(path, ext_separator) > 0:
        return os.path.splitext(path)[0]
    else:
        return ""

# Dirs specific
def is_dir(path:str) -> bool:
    return os.path.isdir(path)

def is_not_dir(path:str) -> bool:
    return not os.path.isdir(path)

def is_parent(path: str, of_path: str) -> bool: # not used
    if is_not_dir(path) or is_not_dir(of_path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    abs_path = get_abs_path(path)
    abs_of_path = get_abs_path(of_path)
    common_path = get_common_path([abs_path, abs_of_path])
    return abs_path == common_path and abs_path != abs_of_path

def get_root_dir(path: str) -> str: # not used
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
        for root, _ , _ in tqdm(os.walk(path), unit=" dir", desc="Computing branch depth")
    )

def iter_dir_hierarchy(path: str, max_depth_from_dir: int) -> Iterator[tuple[int, str]]:
    if is_not_dir(path):
        raise NotADirectoryError(f"Provided path '{path}' is not a dir")
    max_depth_from_root = max_depth_from_dir + get_dir_depth(path)
    for root, dirs, files in tqdm(os.walk(path), unit=" dir", desc="Scanning dir hierarchy"):
        current_depth = get_dir_depth(root)
        if current_depth >= max_depth_from_root:
            dirs[:] = []
        yield current_depth, root, files
