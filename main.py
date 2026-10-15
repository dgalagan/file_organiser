import argparse
from enum import StrEnum, auto
from cli.components import Notifications, Warnings, Errors, Prompt, TQDMDesc
from cli.tokens import Color
from constants import PROJECT_ROOT, OUTPUT_DIR_PATH, REGISTER_PATH, METADATA_PATH, EXTENSION_MAP_PATH, DIR_SCHEMA_PATH
from core.cache import Cache
from core.col_names import ColNames, dest
from core.col_filter_presets import registry_cols, metadata_cols, date_cols
from core.parser import DateParser
from core.exif import Exif
from core.categories import Category, CategorySelection
from core.pipelines import assemble_file_path, add_stat, add_file_id, consolidate_file_ext, build_dims_calculator, assemble_dest_dir
from dataclasses import dataclass
from dataframe.pipeline import FilterRows
from dataframe.predicate import Condition
from dataframe.write import CSVWriter, JSONWriter
from dataframe.load import JSONLoader
from datetime import datetime
import json
import os
import pandas as pd
from reverse_geocoder import RGeocoder
import shutil
from tqdm import tqdm
from typing import Callable, Literal
from utils.path import get_drive, iter_dir_tree, tree_depth, depth_from_drive, is_parent, is_dir, is_empty, move, copy
from collections import defaultdict

###############################
############ TO-DO ############
###############################

# [info] with shutil.copy2 atime and ctime updated, mtime preserved
# [info] CacheKey blends inodedev, inode
# [info] File Category becomes primary key and will be used as base dir hierarchy dimension under which all other dimensions will be stored

# [scan_directories] instead of os.walk(), create recursion based on os.scandir()
# [scan_directories] try while loop / stack approach
# [df] rename Predicate class into RowMask or RowFilter, remove where from Compute and Transform
# [df] develop partial hash function
# [df] in Combined filter if selected empty return AllCols
# [df] ensure coherence of dtypes between different steps in df processing
# [df] check read csv encoding cp852 for ref table read
# [categories] validate literal list against ref table

EXIFTOOL_PATH = "D:/Development/Software/Projects/file_organiser/bin/exif/exiftool(-k).exe"
EXIFTOOL_ARGS = ["-j", "-G", "-all", "--File:Directory"]
EXIFTOOL_ENCODING = "utf-8"
EXIFTOOL_BATCH_SIZE = 50
TQDM_BAR = '{l_bar}{bar:60}{r_bar}{bar:-10b}'

# [Category 1]
#   |__FileHashDupLabel
#      |__EarliestYear
#         |__Category Specific Dim
# [Category 2]
#   |__FileHashDupLabel
#      |__EarliestYear
#         |__Category Specific Dim

@dataclass
class Config:
    register: Cache
    metadata: Cache
    ref: pd.DataFrame
    exif: Exif
    csv_writer: CSVWriter
    dir_schema: dict[str: list[tuple[str, bool]]]
    dims_calculator: dict[str, Callable]

@dataclass
class DirLoc:
    level: int # position on the global ruler (levels from drive)
    depth: int # subtree depth below dir (dir itself = 0)

@dataclass
class DirProcessingConfig:
    start_depth: int #  next layer following the layers covered by a parent (dir = 0)
    target_depth: int # level to which the dir is traversed (0 = dir only)

class MenuActions(StrEnum):
    EXIT = auto()
    INTERRUPT = auto()
    SKIP = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

###############################
########### HELPERS ###########
###############################
def inspect_directories(dir_paths: list[str]) -> dict[str, DirLoc]:
    unique_dir_paths = set(os.path.normpath(dir_path) for dir_path in dir_paths)
    dir_loc_map: dict[str, DirLoc] = {}
    for dir_path in unique_dir_paths:
        if not is_dir(dir_path) or is_empty(dir_path):
            continue
        dir_loc_map[dir_path] = DirLoc(
            level=depth_from_drive(dir_path),
            depth = tree_depth(dir_path)
        )
    return dir_loc_map

def show_directories_tree(dir_loc_map: dict[str, DirLoc], coverage_bar: dict[str, str] = None) -> None:
    # Sort by path name
    dir_loc_map = dict(sorted(dir_loc_map.items(), key=lambda item: item[0]))
    # Handle depth bar
    coverage_bar = coverage_bar or {}
    # Containers
    dir_structure: list[str] = []
    seen_paths: list[str] = []
    
    # Execution
    for path, dir_loc in dir_loc_map.items():
        # Check parents in seen
        parent = None
        parent_lvl = -1
        parent_count = 0
        for seen_path in seen_paths:
            seen_lvl = dir_loc_map[seen_path].level
            if is_parent(seen_path, path):
                parent_count += 1
                if seen_lvl > parent_lvl:
                    parent, parent_lvl = seen_path, seen_lvl
        depth = dir_loc.depth
        bar = coverage_bar.get(path, (depth + 1) * "|")
        pad = " " * max(0, 20 - (depth + 1))
        if parent:
            rel_path = os.path.relpath(path, parent)
            dir_structure.append(f"{bar}{pad}{parent_count * '  '}|_{rel_path}")
        else:
            dir_structure.append(f"{bar}{pad}{path}")
        seen_paths.append(path)

    # Print tree
    print("\n".join(dir_structure))

def build_processing_configs(dir_loc_map: dict[str, DirLoc]) -> dict[str, DirProcessingConfig]: # dependency: prompt_depth() 
    # Sort by path name
    dir_loc_map = dict(sorted(dir_loc_map.items(), key=lambda item: item[0]))
    # Depth bar colors
    input_color = Color.GREEN
    covered_color = Color.LIGHT_GREEN
    uncovered_color = Color.GREY
    reset = Color.RESET
    # Containers
    dir_config_map: dict[str, DirProcessingConfig] = {}
    coverage_bar: dict[str, str] = {}

    print(f"\nInput tree")
    show_directories_tree(dir_loc_map)

    print("\n".join(["\nSelect processing depth", f"  [blank]  skip", f"  [Ctrl+C] abort\n"]))
    interrupt = False
    for path, dir_loc in dir_loc_map.items():
        lvl = dir_loc.level
        depth = dir_loc.depth
        # Find deepest processed ancestor.
        parent_lvl, parent_target_depth = -1, -1
        for processed_path, dir_config in dir_config_map.items():
            processed_path_lvl = dir_loc_map[processed_path].level
            if is_parent(processed_path, path) and processed_path_lvl > parent_lvl:
                parent_lvl, parent_target_depth = processed_path_lvl, dir_config.target_depth
        # Estimate how many layers were covered by parent 
        covered_depth = -1
        if parent_lvl >= 0:
            parent_target_lvl = parent_lvl + parent_target_depth
            child_max_lvl = lvl + depth
            if parent_target_lvl >= lvl:
                if parent_target_lvl < child_max_lvl:
                    covered_depth = parent_target_lvl - lvl
                else:
                    covered = covered_color + "|" * (depth + 1) + reset
                    coverage_bar[path] = covered
                    continue
        # Generate bar for interrupt case 
        if interrupt:
            covered = covered_color + "|" * covered_depth + reset
            uncovered = uncovered_color + '|' * (depth - covered_depth) + reset
            coverage_bar[path] = covered + uncovered
            continue
        # Get user input
        depth_range = [covered_depth + 1, depth]
        target_depth, in_action = prompt_depth(path, depth_range)
        # Generate bar for user input
        covered = covered_color + "|" * (covered_depth + 1) + reset
        if target_depth >= 0:
            user = input_color + '|' * (target_depth - covered_depth) + reset
            uncovered = uncovered_color + '|' * (depth - target_depth) + reset
        else:
            user = ''
            uncovered = uncovered_color + '|' * (depth - covered_depth) + reset
        coverage_bar[path] = covered + user + uncovered

        match in_action:
            case MenuActions.SKIP:
                continue
            case MenuActions.SUCCESS:
                dir_config_map[path] = DirProcessingConfig(
                    start_depth=covered_depth + 1,
                    target_depth=target_depth
                )
            case MenuActions.INTERRUPT:
                interrupt = True

    if not dir_config_map:
        raise ValueError(Errors.ELEMENTS["empty"].build(subject="src roots")) #--- Error ---

    print(f"\nOutput tree")
    show_directories_tree(dir_loc_map, coverage_bar)
    
    return dir_config_map

def prompt_depth(dir_path: str, depth_range: list[int]) -> tuple[int, StrEnum]: # dependency: build_processing_configs()

    range_str = f"{depth_range[0]}-{depth_range[1]}" if depth_range[0] != depth_range[1] else depth_range[0]

    while True:
        try:
            prompt_message = Prompt.ELEMENTS["depth_input"].build(indent=1, dir_path=dir_path, num=range_str)
            offset = len(prompt_message) - len(dir_path) - Prompt.ANSI_LEN
            processing_depth = input(f"{prompt_message}\n{" "*offset}\\__depth: ")
            if processing_depth == "":
                return -1, MenuActions.SKIP
            processing_depth = int(processing_depth)
            if depth_range[0] <= processing_depth <= depth_range[1]:
                return processing_depth, MenuActions.SUCCESS
            print(f"{" "*offset}{Warnings.ELEMENTS["invalid_input"].build()}")
            continue
        except ValueError:
            print(f"{" "*offset}{Warnings.ELEMENTS["invalid_input"].build()}")
            continue
        except KeyboardInterrupt:
            print()
            return -1, MenuActions.INTERRUPT

def collect_dirs_to_delete(dirs_df: pd.DataFrame) -> list[str]:
    dirs_to_del = defaultdict(set)
    for _, row in dirs_df.iterrows():
        relpath = os.path.relpath(row[ColNames.FILE_DIR_PATH], row[ColNames.ROOT])
        if relpath != '.':
            dir_parts = relpath.split(os.sep)
            for level in range(len(dir_parts)): # ensure consistent naming
                dir_to_del = os.path.join(row[ColNames.ROOT], os.sep.join(dir_parts[:level+1]))
                dirs_to_del[level+1].add(dir_to_del)
    return [dir_path for level in sorted(dirs_to_del, reverse=True) for dir_path in dirs_to_del[level]]

def execute_operation(files_df: pd.DataFrame, operation: Callable, register: Cache, metadata: Cache):

    op_name = operation.__name__

    # Execute operation
    tqdm.pandas(desc=TQDMDesc.ELEMENTS[op_name].build(indent=1), bar_format=TQDM_BAR) #------- TQDM ------
    files_df[op_name] = files_df.progress_apply(lambda row: operation(row[ColNames.FILE_PATH], row[dest(ColNames.FILE_PATH)]), axis=1)
    files_df = add_stat(prefix="Dest", metrics=["st_dev", "st_ino"]).run(files_df)
    files_df = add_file_id(prefix="Dest").run(files_df)

    n_total = len(files_df)
    n_succeeded = len(files_df[op_name].loc[files_df[op_name].isna()])
    n_failed = len(files_df[op_name].loc[files_df[op_name].notna()])
    print(Notifications.ELEMENTS["op_done"].build(indent=3, n=n_succeeded, n_total=n_total, share=n_succeeded/n_total)) #--- Notification ---
    print(Notifications.ELEMENTS["op_failed"].build(indent=3, n=n_failed, n_total=n_total, share=n_failed/n_total)) #--- Notification ---

    # Remove emptied dirs
    errors = []
    if operation is move:
        dirs_df = files_df[[ColNames.ROOT, ColNames.FILE_DIR_PATH]].drop_duplicates()
        dirs_to_del = collect_dirs_to_delete(dirs_df)
        for dir_to_del in tqdm(dirs_to_del, desc=TQDMDesc.ELEMENTS["remove"].build(indent=1), bar_format=TQDM_BAR): #--- TQDM ---
            try:
                os.rmdir(dir_to_del)
            except OSError as e:
                errors.append(Errors.ELEMENTS["exception"].build(indent=3, path=dir_to_del, errno=e.errno)) #--- Error ---
    if errors:
        print("\n".join(errors))

    # Post operation cache sync
    # Identify successfully completed operation cases
    completed = files_df.loc[files_df[operation.__name__].isna(), [ColNames.FILE_ID, dest(ColNames.FILE_ID), dest(ColNames.FILE_PATH), dest(ColNames.INODE_DEV), dest(ColNames.INODE)]]
    completed = completed.rename(
        columns = {
            dest(ColNames.FILE_PATH): ColNames.FILE_PATH,
            dest(ColNames.INODE_DEV): ColNames.INODE_DEV,
            dest(ColNames.INODE): ColNames.INODE,
        }
    )
    # Check file id change post operation
    # If file id changed (move to another drive, copy) clone cache record from old to new id and update entry, delete old ones if move
    # If no change (move within drive) update ffile path only
    no_chg_id = completed.loc[completed[ColNames.FILE_ID] == completed[dest(ColNames.FILE_ID)]]
    no_chg_id = no_chg_id[[ColNames.FILE_ID, ColNames.FILE_PATH]].set_index(ColNames.FILE_ID)

    chg_id = completed.loc[completed[ColNames.FILE_ID] != completed[dest(ColNames.FILE_ID)]]
    src_to_dest = dict(zip(chg_id[ColNames.FILE_ID], chg_id[dest(ColNames.FILE_ID)]))
    chg_id = chg_id[[dest(ColNames.FILE_ID), ColNames.FILE_PATH, ColNames.INODE_DEV, ColNames.INODE]].set_index(dest(ColNames.FILE_ID))

    # Update cache
    for cache in (register, metadata):
        if not no_chg_id.empty:
            cache.update(no_chg_id.convert_dtypes()) # ensure dtype alignment
        if not chg_id.empty:
            cache.clone(src_to_dest)
            cache.update(chg_id.convert_dtypes()) # ensure dtype alignment
            if operation is move:
                # drop stale cache entries
                stale_ids = list(src_to_dest.keys())
                cache.delete(stale_ids)

    return files_df

def bytes_converter(n_bytes: int, unit: Literal["MB", "GB", "TB"]) -> int:
    match unit:
        case "MB": return int(n_bytes / 1024 ** 2)
        case "GB": return int(n_bytes / 1024 ** 3)
        case "TB": return int(n_bytes / 1024 ** 4)

def load_json(path: str):
    with open(path, 'r') as f:
        return json.load(f)

###############################
####### MAIN FUNCTIONS ########
###############################

def restore(
        report_name: str,
        operation: Callable,
        config: Config
    ) -> pd.DataFrame:

    op_name = operation.__name__

    if operation not in (copy, move):
        raise ValueError(Errors.ELEMENTS["unknown_value"].build(received=op_name, expected=[op.__name__ for op in (copy, move)]))

    if operation is move:
        base = Warnings.ELEMENTS["base"].build()
        offset = len(base) - Warnings.ANSI_LEN
        response = input(f"{base}Source files will be permanently deleted\n{" "*offset}\\__Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()

    # Load cache
    register, metadata = config.register, config.metadata

    for cache in (register, metadata):
        cache.load()

    report_path = os.path.join(OUTPUT_DIR_PATH, report_name)
    if not os.path.exists(report_path):
        raise FileNotFoundError(f"Report not found: {report_path}")
    files_df = pd.read_csv(report_path)
    if files_df.empty:
        raise ValueError(Errors.ELEMENTS["empty"].build(subject="files")) #--- Error ---
    files_df = files_df[[dest(ColNames.ROOT),dest(ColNames.FILE_ID), dest(ColNames.FILE_PATH), dest(ColNames.FILE_DIR_PATH), ColNames.FILE_PATH]]
    files_df = files_df.rename(columns={
        dest(ColNames.ROOT): ColNames.ROOT,
        dest(ColNames.FILE_ID): ColNames.FILE_ID,
        dest(ColNames.FILE_DIR_PATH): ColNames.FILE_DIR_PATH,
        dest(ColNames.FILE_PATH): ColNames.FILE_PATH,
        ColNames.FILE_PATH: dest(ColNames.FILE_PATH)
    })
    files_df = execute_operation(files_df, operation, register, metadata)

    # Save summary
    files_df = files_df.dropna(axis="columns", how="all")
    datestamp = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")
    summary_path = os.path.join(OUTPUT_DIR_PATH, f"restore_{datestamp}.csv")
    config.csv_writer.save(files_df, summary_path)
    print(f"\n{Notifications.ELEMENTS["save_done"].build(path=os.path.relpath(summary_path, PROJECT_ROOT))}") #--- Notification ---
    # Save cache
    register.save(dropna=False)
    metadata.save(dropna=True)

    return files_df

def organise(
        src_roots: list[str],
        dest_root: str,
        operation: Callable,
        config: Config,
        file_categories: list[Category] = None,
        clear_cache: bool = False,
    ) -> pd.DataFrame:

    op_name = operation.__name__
    is_cross_drive = {get_drive(src_root) for src_root in src_roots} != {get_drive(dest_root)}
    drive_scope = "ACROSS DRIVES" if is_cross_drive else "ON THE SAME DRIVE"
    print(f"{op_name.upper()} FILES {drive_scope}")

    if operation not in (copy, move):
        raise ValueError(Errors.ELEMENTS["unknown_value"].build(received=op_name, expected=[op.__name__ for op in (copy, move)])) #--- Error ---

    if operation is move:
        base = Warnings.ELEMENTS["base"].build()
        offset = len(base) - Warnings.ANSI_LEN
        response = input(f"{base}Source files will be permanently deleted\n{" "*offset}\\__Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()

    # Load cache
    register, metadata = config.register, config.metadata
    for cache in (register, metadata):
        if clear_cache:
            cache.clear()
        else:
            cache.load()

    # Load services
    exif = config.exif

    # Load ref, dir_schema
    dir_schema = config.dir_schema

    # Validate and select source roots
    root_locs = inspect_directories(src_roots)

    if len(src_roots) == 1 and src_roots[0] == dest_root:
        file_categories = None
        processing_configs = {path: DirProcessingConfig(start_depth=0, target_depth=dir_loc.depth) for path, dir_loc in root_locs.items()}
    else:
        processing_configs = build_processing_configs(root_locs)

    # Extract files to process
    file_records = []
    for path, path_config in processing_configs.items():
        for depth, dirpath, filenames in iter_dir_tree(path, path_config.start_depth, path_config.target_depth):
            for filename in filenames:
                file_records.append((path, path_config.target_depth, dirpath, depth, filename))

    if not file_records:
        raise ValueError(Errors.ELEMENTS["empty"].build(subject="files")) #--- Error ---

    # Pre-processing
    files_df = pd.DataFrame(file_records, columns=[ColNames.ROOT, ColNames.ROOT_PROCESSING_DEPTH, ColNames.FILE_DIR_PATH, ColNames.FILE_DIR_DEPTH, ColNames.FILE_NAME])

    #--- Notification ---
    print("\nFiles found")
    for root in files_df[ColNames.ROOT].unique():
        print(Notifications.ELEMENTS["root_stat"].build(indent=1, dir_path=root, n=len(files_df.loc[files_df[ColNames.ROOT] == root])))
    #--- Notification ---

    files_df[ColNames.EXIF_ARGS] = "".join(EXIFTOOL_ARGS)
    files_df[dest(ColNames.ROOT)] = dest_root
    files_df = assemble_file_path(prefix="").run(files_df)
    files_df = add_stat(prefix="", metrics=["st_size", "st_mtime", "st_dev", "st_ino"]).run(files_df)
    files_df = add_file_id(prefix="").run(files_df)

    # Check if there is enough space to process files
    if operation is copy or (operation is move and is_cross_drive):
        required = files_df[ColNames.SIZE].sum()
        _, _, free = shutil.disk_usage(dest_root)
        if required >= free:
            required_gb = bytes_converter(required, "GB")
            free_gb = bytes_converter(free, "GB")
            raise RuntimeError(Errors.ELEMENTS["low_disk_space"].build(op=op_name, required=required_gb, free=free_gb)) #--- Error ---

    new_files_df = files_df[~files_df[ColNames.FILE_ID].isin(register.data.index)].set_index(ColNames.FILE_ID)
    known_files_df = files_df[files_df[ColNames.FILE_ID].isin(register.data.index)].set_index(ColNames.FILE_ID)

    # Identify changed files
    changed_files_df = pd.DataFrame()
    if not known_files_df.empty:
        date_change = register.data.loc[known_files_df.index, ColNames.MODIFIED_AT] != known_files_df[ColNames.MODIFIED_AT] # risky check for float type
        size_change = register.data.loc[known_files_df.index, ColNames.SIZE] != known_files_df[ColNames.SIZE]
        args_change = register.data.loc[known_files_df.index, ColNames.EXIF_ARGS] != known_files_df[ColNames.EXIF_ARGS]
        changed_files_df = known_files_df.loc[date_change | size_change | args_change]

    print("\nFiles processing")
    n_total = len(files_df)
    n_loaded = len(known_files_df) - len(changed_files_df)
    print(Notifications.ELEMENTS["cache_load"].build(indent=1, n=n_loaded, n_total=n_total, share=n_loaded/n_total))#--- Notification ---

    # Extract exif metadata
    to_exif_df = pd.concat([new_files_df, changed_files_df]).reset_index()[[ColNames.FILE_PATH, ColNames.FILE_ID, ColNames.INODE_DEV, ColNames.INODE]]
    if not to_exif_df.empty:
        files_to_exif = to_exif_df[ColNames.FILE_PATH].to_list()
        n_files = len(files_to_exif)
        exif_results = list(tqdm(exif.extract(files_to_exif, args=EXIFTOOL_ARGS), total=n_files, desc=TQDMDesc.ELEMENTS["extract"].build(indent=1), bar_format=TQDM_BAR)) #------- TQDM ------
        exif_df = pd.DataFrame(exif_results)
        exif_df["SourceFile"] = exif_df["SourceFile"].apply(os.path.normpath)
        exif_df = exif_df.merge(to_exif_df, how="left", left_on="SourceFile", right_on=ColNames.FILE_PATH)
        exif_df = exif_df.drop(columns="SourceFile")
        exif_df = exif_df.set_index(ColNames.FILE_ID)

    # Update cache
    if not changed_files_df.empty:
        register.update(changed_files_df[registry_cols.select(changed_files_df.columns)].convert_dtypes()) # ensure dtype alignment
        metadata.update(exif_df.loc[exif_df.index.isin(changed_files_df.index)].convert_dtypes()) # ensure dtype alignment

    if not new_files_df.empty:
        register.add(new_files_df[registry_cols.select(new_files_df.columns)])
        metadata.add(exif_df.loc[exif_df.index.isin(new_files_df.index)])

    # Select metadata
    metadata_df = metadata.data[metadata.data.index.isin(files_df[ColNames.FILE_ID])]
    metadata_df = metadata_df[metadata_cols.select(metadata_df.columns)]

    # Enrich files with exif metadata
    files_df = files_df.merge(metadata_df, how="left", left_on=ColNames.FILE_ID, right_index=True)
    files_df = consolidate_file_ext().run(files_df)

    # Get categories from ref
    files_df = files_df.merge(config.ref[[ColNames.FILE_EXT, ColNames.FILE_CATEGORY]], how="left", left_on=ColNames.CONSOLIDATED_EXT, right_on=ColNames.FILE_EXT)
    files_df[ColNames.FILE_CATEGORY] = files_df[ColNames.FILE_CATEGORY].fillna("Other")

    # Filter file category
    if file_categories:
        files_df = FilterRows(Condition(ColNames.FILE_CATEGORY, "isin", file_categories)).run(files_df)
        n_filtered = n_total - len(files_df)
        print(Notifications.ELEMENTS["filtered"].build(indent=1, n=n_filtered, n_total=n_total, share=n_filtered/n_total)) #--- Notification ---
        if files_df.empty:
            raise ValueError(Errors.ELEMENTS["empty"].build(subject="files")) #--- Error ---

    # Resolve dest dir schema
    unique_categories = files_df[ColNames.FILE_CATEGORY].unique()
    dispatched_dir_schema = {cat: [dim for dim, enabled in dir_schema.get(cat, dir_schema["Default"]) if enabled] for cat in unique_categories}
    unique_dims = set(sum(dispatched_dir_schema.values(), []))
    
    for dim in unique_dims:
        if dim in files_df.columns:
            continue
        elif dim not in config.dims_calculator:
            print(Errors.ELEMENTS["unknown_value"].build(received=dim, expected=config.dims_calculator.keys()))
            continue
        files_df = config.dims_calculator[dim].run(files_df)
    
    for cat, dims in dispatched_dir_schema.items():
        requested_dims = [ColNames.ROOT, ColNames.FILE_CATEGORY] + dims if dims else []
        if dims:
            files_df = assemble_dest_dir(requested_dims, cat).run(files_df)
        else:
            files_df = assemble_dest_dir(requested_dims, cat).run(files_df)

    # Assemble dest file path
    files_df = assemble_file_path(prefix="Dest").run(files_df)

    # Execute operation
    files_df = execute_operation(files_df, operation, register, metadata)

    # Save summary
    files_df = files_df.dropna(axis="columns", how="all")
    datestamp = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")
    summary_path = os.path.join(OUTPUT_DIR_PATH, f"organise_{datestamp}.csv")
    config.csv_writer.save(files_df, summary_path)
    print(f"\n{Notifications.ELEMENTS["save_done"].build(path=os.path.relpath(summary_path, PROJECT_ROOT))}") #--- Notification ---
    # Save cache
    register.save(dropna=False)
    metadata.save(dropna=True)

    return files_df

def main():
    parser = argparse.ArgumentParser(description="File organiser")
    parser.add_argument("action", choices=["organise", "restore"])
    args = parser.parse_args()

    # subparsers = parser.add_subparsers(dest="action", required=True)
    # org = subparsers.add_parser("organise")
    # res = subparsers.add_parser("restore")

    # # organise command
    # org.add_argument("--src", nargs="+", required=True, help="source root(s)")
    # org.add_argument("--dest", required=True, help="destination root")
    # org.add_argument("--operation", choices=["copy", "move"], default="copy")
    # org.add_argument("--clear-cache", action="store_true")

    # # restore command
    # res.add_argument("--report", required=True, help="report file name")
    # res.add_argument("--operation", choices=["copy", "move"], default="move")

    json_loader = JSONLoader(orient="index")
    json_writer = JSONWriter(orient="index", force_ascii=False)

    config = Config(
        register=Cache(path=REGISTER_PATH, writer=json_writer, loader=json_loader),
        metadata=Cache(path=METADATA_PATH, writer=json_writer, loader=json_loader),
        ref=pd.read_csv(EXTENSION_MAP_PATH, encoding="cp852"), # review  encoding issue
        exif=Exif(path=EXIFTOOL_PATH, encoding=EXIFTOOL_ENCODING, batch_size=EXIFTOOL_BATCH_SIZE),
        csv_writer=CSVWriter(encoding="utf-8-sig"),
        dir_schema=load_json(DIR_SCHEMA_PATH),
        dims_calculator=build_dims_calculator(geocoder=RGeocoder(mode=1, verbose=False), date_parser=DateParser(), date_col_filter=date_cols)
    )

    category_selection = CategorySelection().get()

    if args.action == "organise":
       organise(
            # src_roots=["D:\\HDD Data\\Ciklum", "D:\\OneDrive", "D:\\HDD Data", "D:\\OneDrive\\Desktop\\Books", "D:\\HDD Data\\Ciklum\\Adidas", "D:\\HDD Data\\CurriculumVitae", "D:\\HDD Data\\OTHER", "D:\\HDD Data\\OTHER\\Flashka 2\\ТПК2\\Презентации\\Рассылка на КОК"],
            src_roots=["D:\\MyOrganizedFiles"],
            dest_root="D:\\MyOrganizedFiles",
            operation=move,
            config=config,
            file_categories=category_selection,
            clear_cache=False,
        )

    elif args.action == "restore":
        restore(
            report_name="organise_20260916T145535.csv",
            operation=move,
            config=config
        )
    else:
        print(args.action)

if __name__ == "__main__":
    main()