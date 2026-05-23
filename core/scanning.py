import os
from utils.path import iter_dir_hierarchy

def get_scope(input_dirs: list[str]):
    dirs = set()
    files = set()
    for input_dir, max_depth in input_dirs:
        print(f"Exrtacting files from '{input_dir}'")
        dirs_counter = 0
        files_counter = 0
        for depth, dir, filenames in iter_dir_hierarchy(input_dir, max_depth):
            dirs.add(dir)
            dirs_counter += 1
            for filename in filenames:
                file_path = os.path.join(dir, filename)
                files.add(file_path)
                files_counter += 1
        print(f"↓↓↓")
        print(f"{dirs_counter} dirs scanned, {files_counter} files extracted")
        print(f"--------------------------------------------------------------------------------")
    return dirs, files

