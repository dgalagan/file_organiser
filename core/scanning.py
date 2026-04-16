from functools import partial
import os
from tqdm import tqdm
from utils.path import iter_dir_hierarchy

def get_scope(input_dirs):
    dirs = []
    files = []
    for input_dir, max_depth in input_dirs:
        desc = f"Extract files from {input_dir}"
        with tqdm(unit=" dir", desc=f"{desc:<30}") as pbar:
            for depth, dir, dir_files in iter_dir_hierarchy(input_dir, max_depth, pbar=pbar):
                dirs.append(dir)
                file_path = [os.path.join(dir, dir_file) for dir_file in dir_files]
                files.extend(file_path)
    return dirs, files

