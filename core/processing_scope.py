from cli.tokens import Icon, Separator
from utils.path import iter_dir_hierarchy
import os
from tqdm import tqdm

def collect_files_to_organise(input_dirs: list[tuple]):
    dirs = set()
    files = set()
    tqdm_desc = "Extracting files from input dirs:"
    dirs_counter = 0
    files_counter = 0
    for input_dir, max_depth in tqdm(input_dirs, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        for depth, dir, filenames in iter_dir_hierarchy(input_dir, max_depth):
            dirs.add(dir)
            dirs_counter += 1
            for filename in filenames:
                file_path = os.path.join(dir, filename)
                files.add(file_path)
                files_counter += 1
    print(Separator.DASH.repeat(100))
    print(f"{dirs_counter} dirs scanned, {files_counter} files extracted")
    print(Icon.DOWNARROW.repeat(3))
    return dirs, files