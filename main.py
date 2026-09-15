from enum import StrEnum, auto
from cli.components import Notifications, Warnings, Errors, Prompt, TQDMDesc
from cli.tokens import Color
from core.parser import DateParser
from core.cache import Cache
from core.exif import Exif
from core.categories import Category, CategorySelection
from core.pipelines import assemble_file_path, add_stat, add_file_id, consolidate_file_ext, prepare_dimensions_calc, assemble_dest_dir
from core.tagstore import TagStore
from constants import Tags, Cols, PROJECT_ROOT, OUTPUT_DIR_PATH, REGISTER_PATH, METADATA_PATH, EXTENSION_MAP_PATH
from dataclasses import dataclass
from dataframe.pipeline import FilterRows
from dataframe.col_filter import ColumnFilter, NameFilter, KeywordFilter, CombinedFilter
from dataframe.predicate import Condition
from dataframe.write import CSVWriter, JSONWriter
from dataframe.load import JSONLoader
from datetime import datetime
import os
import pandas as pd
from reverse_geocoder import RGeocoder
import shutil
from tqdm import tqdm
from typing import Callable, Literal
from utils.path import iter_dir_tree, tree_depth, depth_from_drive, is_parent, is_dir, is_empty, move, copy
from collections import defaultdict

###############################
############ TO-DO ############
###############################

# [info] with shutil.copy2 atime and ctime updated, mtime preserved
# [info] CacheKey blends inodedev, inode

# [scan_directories] instead of os.walk(), create recursion based on os.scandir()
# [scan_directories] try while loop / stack approach
# [df] rename Predicate class into RowMask or RowFilter, remove where from Compute and Transform
# [df] develop partial hash function
# [df] in Combined filter if selected empty return AllCols
# [df] ensure coherence of dtypes between different steps in df processing
# [categories] validate literal list against ref table

EXIFTOOL_PATH = "D:/Development/Software/Projects/file_organiser/bin/exif/exiftool(-k).exe"
EXIFTOOL_ARGS = ["-j", "-G", "-all", "--File:Directory"]
EXIFTOOL_ENCODING = "utf-8"
EXIFTOOL_BATCH_SIZE = 50
META_DATE_TAGS: list[str] = [Tags.CREATE_DT, Tags.ACCESS_DT, Tags.MODIFY_DT]
META_TAGS_TO_COLS: dict[str, ColumnFilter] = {
    Tags.CREATE_DT: CombinedFilter([
        NameFilter([Cols.ID3_YEAR, Cols.EXE_TIMESTAMP, Cols.XMP_TIMESTAMP, Cols.PNG_DATETIME, Cols.COMPOSITE_DATETIME, Cols.QT_PURCHASE_DATE]),
        KeywordFilter(["createdate", "creationdate", "createddatetime", "datetimeoriginal", "datetimedigitized", "datetimecreated"])
        # "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
    ]),
    Tags.ACCESS_DT: KeywordFilter(["accessdate", "lastplayed", "lastprinted"]),
    Tags.MODIFY_DT: KeywordFilter(["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"]),
}
DIR_SCHEMA = {
    "Universal": [("FileHashDupLabel", True), ("FileCategory", True), ("EarliestYear", True)],
    "Image": [("ImageCountry", True), ("EXIF:Model", True)],
    "Data-Excel": [("WorksheetsCount", True)]
}
TQDM_BAR = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
INDENT = "  "

@dataclass
class Config:
    register: Cache
    metadata: Cache
    ref: pd.DataFrame
    exif: Exif
    csv_writer: CSVWriter
    geocoder: RGeocoder
    parser: DateParser

@dataclass
class DirLoc:
    path: str
    level: int # position on the global ruler (levels from drive)
    depth: int # subtree depth below dir (dir itself = 0)

@dataclass
class DirProcessingConfig:
    loc: DirLoc
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
def inspect_directories(dir_paths: list[str]) -> list[DirLoc]:
    unique_dir_paths = set(os.path.normpath(dir_path) for dir_path in dir_paths)
    dir_locs = []
    for dir_path in unique_dir_paths:
        if not is_dir(dir_path) or is_empty(dir_path):
            continue
        dir_locs.append(DirLoc(dir_path, depth_from_drive(dir_path), tree_depth(dir_path)))
    return dir_locs

def show_directories_tree(dir_locs: list[DirLoc], coverage_bar: dict[str, str] = None) -> None:
    # Sort by path name
    dir_locs = sorted(dir_locs, key=lambda dir_loc: dir_loc.path)
    # Handle depth bar
    coverage_bar = coverage_bar or {}
    # Containers
    dir_structure = []
    seen: list[DirLoc] = []

    # Execution
    for dir_loc in dir_locs:
        path = dir_loc.path
        depth = dir_loc.depth
        # Check parents in seen
        parent = None
        parent_lvl = -1
        parent_count = 0
        for seen_loc in seen:
            seen_path = seen_loc.path
            seen_lvl = seen_loc.level
            if is_parent(seen_path, path):
                parent_count += 1
                if seen_lvl > parent_lvl:
                    parent, parent_lvl = seen_path, seen_lvl
        bar = coverage_bar.get(path, (depth + 1) * "|")
        pad = " " * max(0, 20 - (depth + 1))
        if parent:
            rel_path = os.path.relpath(path, parent)
            dir_structure.append(f"{bar}{pad}{parent_count * '  '}|_{rel_path}")
        else:
            dir_structure.append(f"{bar}{pad}{path}")
        seen.append(dir_loc)

    # Print tree
    print("\n".join(dir_structure))

def build_processing_configs(dir_locs: list[DirLoc]) -> list[DirProcessingConfig]: # dependency: prompt_depth() 
    # Sort by path name
    dir_locs = sorted(dir_locs, key=lambda dir_loc: dir_loc.path)
    # Depth bar colors
    input_color = Color.GREEN
    covered_color = Color.LIGHT_GREEN
    uncovered_color = Color.GREY
    reset = Color.RESET
    # Containers
    processing_configs: list[DirProcessingConfig] = []
    coverage_bar = {}

    print(f"\nInput tree")
    show_directories_tree(dir_locs)

    print("\n".join(["\nSelect processing depth", f"{INDENT}[blank]  Skip", f"{INDENT}[Ctrl+C] Abort\n"]))
    interrupt = False
    for dir_loc in dir_locs:
        path = dir_loc.path
        lvl = dir_loc.level
        depth = dir_loc.depth
        # Check parents in seen
        parent_config, parent_lvl = None, -1
        for processing_config in processing_configs:
            if is_parent(processing_config.loc.path, path) and processing_config.loc.level > parent_lvl:
                parent_config, parent_lvl = processing_config, processing_config.loc.level
        # Estimate how many layers were covered by parent 
        covered_depth = -1
        if parent_config:
            parent_processing_lvl = parent_lvl + parent_config.end_depth
            child_max_lvl = lvl + depth
            if parent_processing_lvl >= lvl:
                if parent_processing_lvl < child_max_lvl:
                    covered_depth = parent_processing_lvl - lvl
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
        processing_depth, in_action = prompt_depth(path, depth_range)
        # Generate bar for user input
        covered = covered_color + "|" * (covered_depth + 1) + reset
        if processing_depth >= 0:
            user = input_color + '|' * (processing_depth - covered_depth) + reset
            uncovered = uncovered_color + '|' * (depth - processing_depth) + reset
        else:
            user = ''
            uncovered = uncovered_color + '|' * (depth - covered_depth) + reset
        coverage_bar[path] = covered + user + uncovered

        match in_action:
            case MenuActions.SKIP:
                continue
            case MenuActions.SUCCESS:
                processing_configs.append(DirProcessingConfig(dir_loc, covered_depth + 1, processing_depth))
            case MenuActions.INTERRUPT:
                interrupt = True

    if not processing_configs:
        raise ValueError(Errors.ELEMENTS["empty_input"].build(subject="src roots")) #--- Error ---

    print(f"\nOutput tree")
    show_directories_tree(dir_locs, coverage_bar)
    
    return processing_configs

def prompt_depth(dir_path: str, depth_range: list[int]) -> tuple[int, StrEnum]: # dependency: build_processing_configs()

    range_str = f"{depth_range[0]}-{depth_range[1]}" if depth_range[0] != depth_range[1] else depth_range[0]

    while True:
        try:
            print(f"{INDENT}{Prompt.ELEMENTS["depth_input"].build(dir_path=dir_path, num=range_str)}")
            processing_depth = input(f"{INDENT*5} \\__depth: ")
            if processing_depth == "":
                return -1, MenuActions.SKIP
            processing_depth = int(processing_depth)
            if depth_range[0] <= processing_depth <= depth_range[1]:
                return processing_depth, MenuActions.SUCCESS
            print(Warnings.ELEMENTS["invalid_input"].build())
            continue
        except ValueError:
            print(Warnings.ELEMENTS["invalid_input"].build())
            continue
        except KeyboardInterrupt:
            print()
            return -1, MenuActions.INTERRUPT

def collect_dirs_to_delete(dirs_df: pd.DataFrame) -> list[str]:
    dirs_to_del = defaultdict(set)
    for row_id, row in dirs_df.iterrows():
        relpath = os.path.relpath(row[Cols.FILE_DIR_PATH], row[Cols.ROOT])
        if relpath != '.': 
            dir_parts = relpath.split(os.sep)
            for level in range(len(dir_parts)):
                dir_to_del = os.path.join(row[Cols.ROOT], os.sep.join(dir_parts[:level+1]))
                dirs_to_del[level+1].add(dir_to_del)
    return [dir_path for level in sorted(dirs_to_del, reverse=True) for dir_path in dirs_to_del[level]]

def execute_operation(files_df: pd.DataFrame, operation: Callable, register: Cache, metadata: Cache, tagstore: TagStore = None):

    op_name = operation.__name__

    # Execute operation
    tqdm.pandas(desc=f"{INDENT}{TQDMDesc.ELEMENTS[op_name].build()}", bar_format=TQDM_BAR) #------- TQDM ------
    files_df[op_name] = files_df.progress_apply(lambda row: operation(row[Cols.FILE_PATH], row[Cols.dest(Cols.FILE_PATH)]), axis=1)
    files_df = add_stat(prefix="Dest", metrics=["st_dev", "st_ino"], tagstore=tagstore).run(files_df)
    files_df = add_file_id(prefix="Dest", tagstore=tagstore).run(files_df)

    # Remove emptied dirs
    if operation is move:
        dirs_df = files_df[[Cols.ROOT, Cols.FILE_DIR_PATH]].drop_duplicates()
        dirs_to_del = collect_dirs_to_delete(dirs_df)
        for dir_to_del in tqdm(dirs_to_del, desc=f"{INDENT}{TQDMDesc.ELEMENTS["remove"].build()}", bar_format=TQDM_BAR): #--- TQDM ---
            try:
                os.rmdir(dir_to_del)
            except OSError as e:
                tqdm.write(Errors.ELEMENTS["exception"].build(op="Remove dir", e=str(e))) #--- Error ---

    n_total = len(files_df)
    n_succeeded = len(files_df[op_name].loc[files_df[op_name].isna()])
    n_failed = len(files_df[op_name].loc[files_df[op_name].notna()])

    print(f"{INDENT}{INDENT}{INDENT}{Notifications.ELEMENTS["op_done"].build(n=n_succeeded, n_total=n_total, share=n_succeeded/n_total)}") #--- Notification ---
    print(f"{INDENT}{INDENT}{INDENT}{Notifications.ELEMENTS["op_failed"].build(n=n_failed, n_total=n_total, share=n_failed/n_total)}") #--- Notification ---
    
    # Post operation cache sync
    # Identify successfully completed operation cases
    completed = files_df.loc[files_df[operation.__name__].isna(), [Cols.FILE_ID, Cols.dest(Cols.FILE_ID), Cols.dest(Cols.FILE_PATH), Cols.dest(Cols.INODE_DEV), Cols.dest(Cols.INODE)]]
    completed = completed.rename(
        columns = {
            Cols.dest(Cols.FILE_PATH): Cols.FILE_PATH,
            Cols.dest(Cols.INODE_DEV): Cols.INODE_DEV,
            Cols.dest(Cols.INODE): Cols.INODE,
        }
    )
    # Check file id change post operation
    # If file id changed (move to another drive, copy) clone cache record from old to new id and update entry, delete old ones if move
    # If no change (move within drive) update ffile path only
    no_chg_id = completed.loc[completed[Cols.FILE_ID] == completed[Cols.dest(Cols.FILE_ID)]]
    no_chg_id = no_chg_id[[Cols.FILE_ID, Cols.FILE_PATH]].set_index(Cols.FILE_ID)

    chg_id = completed.loc[completed[Cols.FILE_ID] != completed[Cols.dest(Cols.FILE_ID)]]
    src_to_dest = dict(zip(chg_id[Cols.FILE_ID], chg_id[Cols.dest(Cols.FILE_ID)]))
    chg_id = chg_id[[Cols.dest(Cols.FILE_ID), Cols.FILE_PATH, Cols.INODE_DEV, Cols.INODE]].set_index(Cols.dest(Cols.FILE_ID))

    # Update cache
    for cache in (register, metadata):
        if not no_chg_id.empty:
            cache.update(no_chg_id) # ensure dtype alignment
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

###############################
####### MAIN FUNCTIONS ########
###############################

def restore(
        report_name: str,
        operation: Callable,
        config: Config
    ) -> pd.DataFrame:

    valid_ops  = (copy, move)
    op_name = operation.__name__

    if operation not in valid_ops:
        raise ValueError(Errors.ELEMENTS["unknown_value"].build(received=op_name, expected=[op.__name__ for op in valid_ops]))

    if operation is move:
        print("MOVE operation selected — original files at the source will be permanently deleted after being moved to the destination")
        response = input("Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()

    # Load cache
    register, metadata = config.register, config.metadata

    for cache in (register, metadata):
        cache.load()

    report_path = os.path.join(OUTPUT_DIR_PATH, report_name)
    if not os.path.exists(report_path):
        raise FileNotFoundError(f"Report not found: {report_path}")
    files_df = pd.read_csv(report_path)[[Cols.dest(Cols.ROOT), Cols.dest(Cols.FILE_ID), Cols.dest(Cols.FILE_PATH), Cols.dest(Cols.FILE_DIR_PATH), Cols.FILE_PATH]]
    files_df = files_df.rename(columns={
        Cols.dest(Cols.ROOT): Cols.ROOT,
        Cols.dest(Cols.FILE_ID): Cols.FILE_ID,
        Cols.dest(Cols.FILE_DIR_PATH): Cols.FILE_DIR_PATH,
        Cols.dest(Cols.FILE_PATH): Cols.FILE_PATH,
        Cols.FILE_PATH: Cols.dest(Cols.FILE_PATH)
    })

    if files_df.empty:
        raise ValueError(Errors.ELEMENTS["empty_input"].build(subject="files")) #--- Error ---

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
        src_roots: str | list[str],
        dest_root: str,
        operation: Callable,
        config: Config,
        file_categories: list[Category] = None,
        dir_schema: dict[str, list[str]] = None,
        clear_cache: bool = False,
    ) -> pd.DataFrame:

    valid_ops = (copy, move)
    op_name = operation.__name__

    if operation not in valid_ops:
        raise ValueError(Errors.ELEMENTS["unknown_value"].build(received=op_name, expected=[op.__name__ for op in valid_ops])) #--- Error ---

    if operation is move:
        # no space consequences
        print("MOVE operation selected — original files at the source will be permanently deleted after being moved to the destination")
        response = input("Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()

    # Init tagstore
    """
    TagStore maps tags to the metadata columns present in a run. The set of possible
    columns is finite, but the subset present varies per run and new files may introduce
    unseen ones. Since assignments are cheap to recompute from the current columns via
    fixed rules, TagStore is built fresh per run and kept in memory rather than persisted.
    """
    tagstore = TagStore()

    # Load cache
    register, metadata = config.register, config.metadata
    for cache in (register, metadata):
        if clear_cache:
            cache.clear()
        else:
            cache.load()

    # Load services
    exif = config.exif
    geocoder = config.geocoder
    date_parser = config.parser

    # Validate and select source roots
    root_locs = inspect_directories(src_roots)
    root_configs = build_processing_configs(root_locs)

    # Extract files to process
    file_records = []
    for root_config in root_configs:
        for depth, dirpath, filenames in iter_dir_tree(root_config.loc.path, root_config.start_depth, root_config.target_depth):
            for filename in filenames:
                file_records.append((root_config.loc.path, root_config.target_depth, dirpath, depth, filename))

    # Pre-processing
    files_df = pd.DataFrame(file_records, columns=[Cols.ROOT, Cols.ROOT_PROCESSING_DEPTH, Cols.FILE_DIR_PATH, Cols.FILE_DIR_DEPTH, Cols.FILE_NAME])

    #--- Notification ---
    print("\nFiles found")
    for root in files_df[Cols.ROOT].unique():
        print(f"{INDENT}{Notifications.ELEMENTS["root_stat"].build(dir_path=root, n=len(files_df.loc[files_df[Cols.ROOT] == root]))}")
    #--- Notification ---

    files_df[Cols.EXIF_ARGS] = "".join(EXIFTOOL_ARGS)
    files_df[Cols.dest(Cols.ROOT)] = dest_root
    files_df = assemble_file_path(prefix="", tagstore=tagstore).run(files_df)
    files_df = add_stat(prefix="", metrics=["st_size", "st_mtime", "st_dev", "st_ino"], tagstore=tagstore).run(files_df)
    files_df = add_file_id(prefix="", tagstore=tagstore).run(files_df)
    reg_cols = NameFilter([Cols.FILE_PATH, Cols.FILE_NAME, Cols.INODE_DEV, Cols.INODE, Cols.MODIFIED_AT, Cols.SIZE, Cols.EXIF_ARGS]).select(files_df.columns)

    # Check if there is enough space to process files
    required = files_df[Cols.SIZE].sum()
    _, _, free = shutil.disk_usage(dest_root)
    if required >= free:
        required_gb = bytes_converter(required, "GB")
        free_gb = bytes_converter(free, "GB")
        raise RuntimeError(Errors.ELEMENTS["low_disk_space"].build(op=op_name, required=required_gb, free=free_gb)) #--- Error ---

    new_files_df = files_df[~files_df[Cols.FILE_ID].isin(register.data.index)].set_index(Cols.FILE_ID)
    known_files_df = files_df[files_df[Cols.FILE_ID].isin(register.data.index)].set_index(Cols.FILE_ID)

    # Identify changed files
    changed_files_df = pd.DataFrame()
    if not known_files_df.empty:
        date_change = register.data.loc[known_files_df.index, Cols.MODIFIED_AT] != known_files_df[Cols.MODIFIED_AT] # risky check for float type
        size_change = register.data.loc[known_files_df.index, Cols.SIZE] != known_files_df[Cols.SIZE]
        args_change = register.data.loc[known_files_df.index, Cols.EXIF_ARGS] != known_files_df[Cols.EXIF_ARGS]
        changed_files_df = known_files_df.loc[date_change | size_change | args_change]

    print("\nFiles processing")
    n_total = len(files_df)
    n_loaded = len(known_files_df) - len(changed_files_df)
    print(f"{INDENT}{Notifications.ELEMENTS["cache_load"].build(n=n_loaded, n_total=n_total, share=n_loaded/n_total)}") #--- Notification ---

    # Extract exif metadata
    to_exif_df = pd.concat([new_files_df, changed_files_df]).reset_index()[[Cols.FILE_PATH, Cols.FILE_ID, Cols.INODE_DEV, Cols.INODE]]
    if not to_exif_df.empty:
        files_to_exif = to_exif_df[Cols.FILE_PATH].to_list()
        n_files = len(files_to_exif)
        exif_results = list(tqdm(exif.extract(files_to_exif, args=EXIFTOOL_ARGS), total=n_files, desc=f"{INDENT}{TQDMDesc.ELEMENTS["extract"].build()}", bar_format=TQDM_BAR)) #------- TQDM ------
        exif_df = pd.DataFrame(exif_results)
        exif_df["SourceFile"] = exif_df["SourceFile"].apply(os.path.normpath)
        exif_df = exif_df.merge(to_exif_df, how="left", left_on="SourceFile", right_on=Cols.FILE_PATH)
        exif_df = exif_df.drop(columns="SourceFile")
        exif_df = exif_df.set_index(Cols.FILE_ID)

    # Update cache
    if not changed_files_df.empty:
        register.update(changed_files_df[reg_cols])
        metadata.update(exif_df.loc[exif_df.index.isin(changed_files_df.index)])

    if not new_files_df.empty:
        register.add(new_files_df[reg_cols])
        metadata.add(exif_df.loc[exif_df.index.isin(new_files_df.index)])

    # Select metadata
    metadata_df = metadata.data[metadata.data.index.isin(files_df[Cols.FILE_ID])]
    date_cols = [col for date_tag in META_DATE_TAGS for col in META_TAGS_TO_COLS[date_tag].select(metadata_df.columns)]
    dims_cols = NameFilter([Cols.FILE_TYPE_EXT, Cols.EXIF_MODEL, Cols.EXIF_GPS_LATITUDE, Cols.EXIF_GPS_LONGITUDE, Cols.XML_HEADING_PAIRS]).select(metadata_df.columns)
    metadata_df = metadata_df[date_cols + dims_cols]

    # Enrich files with exif metadata
    files_df = files_df.merge(metadata_df, how="left", left_on=Cols.FILE_ID, right_index=True)
    files_df = consolidate_file_ext(tagstore=tagstore).run(files_df)

    # Get categories from ref
    files_df = files_df.merge(config.ref[[Cols.FILE_EXT, Cols.FILE_CATEGORY]], how="left", left_on=Cols.CONSOLIDATED_EXT, right_on=Cols.FILE_EXT)
    files_df[Cols.FILE_CATEGORY] = files_df[Cols.FILE_CATEGORY].fillna("Other")

    # Filter file category
    if file_categories:
        files_df = FilterRows(Condition(Cols.FILE_CATEGORY, "isin", file_categories)).run(files_df)
        if files_df.empty:
            raise ValueError(Errors.ELEMENTS["empty_input"].build(subject="files")) #--- Error ---
        n_filtered = n_total - len(files_df)
        print(f"{INDENT}{Notifications.ELEMENTS["filtered"].build(n=n_filtered, n_total=n_total, share=n_filtered/n_total)}") #--- Notification ---

    # Resolve dest dir schema
    if dir_schema:
        resolved_dir_schema = {cat: [dim for dim, enabled in dims if enabled] for cat, dims in dir_schema.items() if cat == "Universal" or cat in files_df[Cols.FILE_CATEGORY].unique()}
        dims_per_category = {cat: resolved_dir_schema["Universal"] + resolved_dir_schema.get(cat, []) for cat in files_df[Cols.FILE_CATEGORY].unique()}
        
        # Calculate dims features
        dims_calc = prepare_dimensions_calc(geocoder, date_cols, date_parser, tagstore=tagstore)
        for dims in resolved_dir_schema.values():
            for dim in dims:
                if dim in files_df.columns:
                    continue
                elif dim in dims_calc:
                    files_df = dims_calc[dim].run(files_df)
                else:
                    raise ValueError(Errors.ELEMENTS["unknown_value"].build(received=dim, expected=dims_calc.keys())) #--- Error ---
        # Asemble dest dir per category
        for cat, dims in dims_per_category.items():
            files_df = assemble_dest_dir(dest_root, file_category=cat, dims=dims, tagstore=tagstore).run(files_df)
    else:
        # Everything goes to root dir
        files_df = assemble_dest_dir(dest_root, tagstore=tagstore).run(files_df)

    # Assemble dest file path
    files_df = assemble_file_path(prefix="Dest", tagstore=tagstore).run(files_df)

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

def main(command: str = "organise"):

    json_loader = JSONLoader(orient="index")
    json_writer = JSONWriter(orient="index", force_ascii=False)

    config = Config(
        register=Cache(path=REGISTER_PATH, writer=json_writer, loader=json_loader),
        metadata=Cache(path=METADATA_PATH, writer=json_writer, loader=json_loader),
        ref=pd.read_csv(EXTENSION_MAP_PATH, encoding="cp852"),
        exif=Exif(path=EXIFTOOL_PATH, encoding=EXIFTOOL_ENCODING, batch_size=EXIFTOOL_BATCH_SIZE),
        csv_writer=CSVWriter(encoding="utf-8-sig"),
        geocoder=RGeocoder(mode=1, verbose=False),
        parser=DateParser(),
    )

    category_selection = CategorySelection().get()

    if command == "organise":
       organise(
            src_roots=["D:\\HDD Data\\Ciklum", "D:\\OneDrive", "D:\\HDD Data", "D:\\OneDrive\\Desktop\\Books", "D:\\HDD Data\\Ciklum\\Adidas", "D:\\HDD Data\\CurriculumVitae", "D:\\HDD Data\\OTHER", "D:\\HDD Data\\OTHER\\Flashka 2\\ТПК2\\Презентации\\Рассылка на КОК"],
            # src_roots = ["D:\\OneDrive"],
            dest_root="D:\\MyOrganizedFiles",
            operation=copy,
            config=config,
            file_categories=category_selection,
            dir_schema=DIR_SCHEMA,
            clear_cache=False,
        )

    elif command == "restore":
        restore(
            report_name="organise_20260831T182240.csv",
            operation=move,
            config=config
        )

if __name__ == "__main__":
    main()
