import argparse
from cli.components import bool_summary, search_count, tqdm_bar
from cli.tokens import Ansi
from constants import OUTPUT_DIR_PATH, REGISTER_PATH, METADATA_PATH, EXTENSION_MAP_PATH, DIR_SCHEMA_PATH
from core.cache import Cache
from core.col_names import ColNames, dest, label, dup
from core.col_filter_presets import  metadata_filter, date_filter
from core.parser import DateParser
from core.exif import Exif
from core.pipelines import add_unique_filename, add_file_path, add_os_stat, add_file_id, consolidate_file_ext, assemble_dest_dir, add_image_country, add_n_worksheets, add_earliest_year, add_duplicate_check, add_labels, add_file_hash
from dataclasses import dataclass
from dataframe.predicate import Condition, AllRows, And
from dataframe.write import CSVWriter, JSONWriter
from dataframe.load import JSONLoader
import time
import os
import pandas as pd
from reverse_geocoder import RGeocoder
from tqdm import tqdm
from utils.checks import ensure_not_empty, ensure_valid, ensure_drive_space
from utils.prompts import request_move_approval, request_depth, MenuActions
from utils.strings import color
from utils.path import get_parents, iter_dir_tree, tree_depth, depth_from_drive, path_depth, is_parent, is_dir, is_empty, move, copy, remove_dir
from utils.json import load_json, save_json

###############################
############ TO-DO ############
###############################

# src_roots=["D:\\HDD Data\\Ciklum", "D:\\OneDrive", "D:\\HDD Data", "D:\\OneDrive\\Desktop\\Books", "D:\\HDD Data\\Ciklum\\Adidas", "D:\\HDD Data\\CurriculumVitae", "D:\\HDD Data\\OTHER", "D:\\HDD Data\\OTHER\\Flashka 2\\ТПК2\\Презентации\\Рассылка на КОК"],

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

# add
# [Category 1]
#   |__FileHashDupLabel
#      |__EarliestYear
#         |__Category Specific Dim
# [Category 2]
#   |__FileHashDupLabel
#      |__EarliestYear
#         |__Category Specific Dim

EXIFTOOL_PATH = "D:/Development/Software/Projects/file_organiser/bin/exif/exiftool(-k).exe"
EXIFTOOL_ARGS = ["-j", "-G", "-all", "--File:Directory"]
EXIFTOOL_ENCODING = "utf-8"
EXIFTOOL_BATCH_SIZE = 50

DEFAULT_DIMENSIONS = [
    dest(ColNames.ROOT),
    ColNames.FILE_CATEGORY
]
CALCULATED_DIMENSIONS = [
    label(dup(ColNames.FILE_HASH)),
    ColNames.EARLIEST_YEAR,
    ColNames.IMAGE_COUNTRY,
    ColNames.WORKSHEETS_COUNT
]
OPERATIONS = {
    "copy": copy,
    "move": move
}
REGISTRY_SCHEMA = {
    ColNames.INODE_DEV: "int64",
    ColNames.ROOT: "str",
    ColNames.FILE_PATH: "str",
    ColNames.MODIFIED_AT: "float64",
    ColNames.SIZE: "int64",
    ColNames.EXIF_ARGS: "str"
}

@dataclass
class Config:
    register: Cache
    metadata: Cache
    ref: pd.DataFrame
    exif: Exif
    csv_writer: CSVWriter
    dir_schema: dict[str, list[tuple[str, bool]]]

@dataclass
class DirLoc:
    level: int # position on the global ruler (levels from drive)
    depth: int # subtree depth below dir (dir itself = 0)

@dataclass
class DirProcessingConfig:
    start_depth: int #  next layer following the layers covered by a parent (dir = 0)
    target_depth: int # level to which the dir is traversed (0 = dir only)

###############################
########### HELPERS ###########
###############################

def latest_run_id() -> str:
    move_history = load_json("cache\\move_history.json")
    run_id = max(move_history.keys())
    if not run_id:
        raise ValueError("No runs to restore")
    return run_id

def inspect_directories(dir_paths: list[str]) -> dict[str, DirLoc]:
    dir_loc_map: dict[str, DirLoc] = {}
    for dir_path in dir_paths:
        if not is_dir(dir_path) or is_empty(dir_path):
            continue
        dir_loc_map[dir_path] = DirLoc(
            level = depth_from_drive(dir_path),
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
    input_color = Ansi.ISLAMIC_GREEN
    covered_color = Ansi.MENTHOL
    uncovered_color = Ansi.GRAY
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
                    coverage_bar[path] = color("|" * (depth + 1), covered_color)
                    continue
        # Generate bar for interrupt case 
        if interrupt:
            covered = color("|" * covered_depth, covered_color)
            uncovered = color('|' * (depth - covered_depth), uncovered_color)
            coverage_bar[path] = covered + uncovered
            continue
        # Get user input
        depth_range = [covered_depth + 1, depth]
        target_depth, in_action = request_depth(path, depth_range)
        # Generate bar for user input
        covered = color("|" * (covered_depth + 1), covered_color)
        if target_depth >= 0:
            user = color("|" * (target_depth - covered_depth), input_color)
            uncovered = color('|' * (depth - target_depth), uncovered_color)
        else:
            user = ''
            uncovered = color('|' * (depth - covered_depth), uncovered_color)
        coverage_bar[path] = covered + user + uncovered

        match in_action:
            case MenuActions.SKIP:
                continue
            case MenuActions.SUCCESS:
                dir_config_map[path] = DirProcessingConfig(start_depth=covered_depth + 1, target_depth=target_depth)
            case MenuActions.INTERRUPT:
                interrupt = True

    ensure_not_empty(dir_config_map, name="src roots")

    print(f"\nTree with selected depth")
    show_directories_tree(dir_loc_map, coverage_bar)
    
    return dir_config_map

def collect_intermediate_dirs(files_df: pd.DataFrame) -> list[str]:
    rm_input = zip(files_df[ColNames.ROOT], files_df[ColNames.FILE_PATH])
    intermediate_dirs = {parent_dir for root_dir, file_path in rm_input for parent_dir in get_parents(file_path, root_dir)}
    return sorted(intermediate_dirs, key=path_depth, reverse=True)

def remove_intermediate_dirs(dir_paths: list[str]) -> pd.Series:
    rm_results = {dir_path: remove_dir(dir_path) for dir_path in tqdm(dir_paths, desc="Remove dirs", bar_format=tqdm_bar())}
    return pd.Series(rm_results, dtype=bool)

def save_op_report(files_df: pd.DataFrame, timestamp) -> None:
    files_df = files_df.dropna(axis="columns", how="all")
    report_path = os.path.join(OUTPUT_DIR_PATH, f"{timestamp}{os.sep}organise.csv") # distinguish between restore and organise
    CSVWriter(encoding="utf-8-sig").save(files_df, report_path)
    print(f"Summary saved -> {report_path}")

def execute_op(files_df: pd.DataFrame, op_name: str) -> pd.Series:
    op_func = OPERATIONS[op_name]
    op_input = zip(files_df.index, files_df[ColNames.FILE_PATH], files_df[dest(ColNames.FILE_PATH)])
    op_results = {
        idx: op_func(src_path, dest_path)
        for idx, src_path, dest_path in tqdm(
            op_input,
            total=len(files_df.index),
            desc=op_name.capitalize(),
            bar_format=tqdm_bar()
        )
    }
    return pd.Series(op_results, dtype=bool)

def run_op_pipeline(files_df: pd.DataFrame, register: Cache, metadata: Cache, op_name: str, timestamp) -> None:
    # needed columns: 
    # ColNames.ROOT, ColNames.INODE_DEV, dest(ColNames.ROOT), dest(ColNames.INODE_DEV)
    # ColNames.FILE_ID, ColNames.FILE_PATH, dest(ColNames.FILE_PATH), ColNames.SIZE
    
    dest_root = files_df[dest(ColNames.ROOT)].iloc[0]

    if op_name == "move":
        # Propmt user approval
        request_move_approval()
        # Check available space on the destination drive
        cross_drive = files_df[ColNames.INODE_DEV] != files_df[dest(ColNames.INODE_DEV)]
        required_b = files_df.loc[cross_drive, ColNames.SIZE].sum()
        ensure_drive_space(dest_root, required_b)
        # Collect dirs
        dir_paths = collect_intermediate_dirs(files_df) 
    elif op_name == "copy":
        # Check available space on the destination drive
        required_b = files_df[ColNames.SIZE].sum()
        ensure_drive_space(dest_root, required_b)

    # Execute operation
    op_results = execute_op(files_df, op_name)
    print(bool_summary(op_results, label={True: "done", False: "failed"}))
    files_df = files_df.loc[op_results]
    files_df = add_os_stat(
        src_col=dest(ColNames.FILE_PATH),
        stat_to_dest_cols={
            "st_ino": dest(ColNames.INODE)
        }
    ).run(files_df)
    files_df = add_file_id([dest(ColNames.INODE_DEV), dest(ColNames.INODE)], dest(ColNames.FILE_ID)).run(files_df)

    if op_name == "move":

        # Remove empty intermediate dirs
        rm_results = remove_intermediate_dirs(dir_paths)
        print(bool_summary(rm_results, label={True: "done", False: "failed"}))

        # Log source root and path 
        move_history = load_json("cache\\move_history.json")
        log_data = files_df[[dest(ColNames.FILE_ID), ColNames.INODE_DEV, ColNames.ROOT, ColNames.FILE_PATH]].set_index(dest(ColNames.FILE_ID))
        move_history[timestamp] = log_data.to_dict(orient="index")
        save_json(move_history, "cache\\move_history.json")
        # history.add(files_df[[dest(ColNames.FILE_ID), ColNames.ROOT, ColNames.FILE_PATH]].set_index(dest(ColNames.FILE_ID)))

        # Define clone and delete scope
        clone = cross_drive[op_results]
        delete = cross_drive[op_results]

    elif op_name == "copy":

        # Define clone and delete scope
        clone = pd.Series(True, index=files_df.index)
        delete = pd.Series(False, index=files_df.index)

    # Update cache
    for cache in (register, metadata):
        cache.clone(dict(zip(files_df.loc[clone, ColNames.FILE_ID], files_df.loc[clone, dest(ColNames.FILE_ID)])))
        cache.delete(files_df.loc[delete, ColNames.FILE_ID])
    
    # Update cloned entries
    cache_upd = files_df[[dest(ColNames.INODE_DEV), dest(ColNames.ROOT), dest(ColNames.FILE_ID), dest(ColNames.FILE_PATH)]]
    cache_upd = cache_upd.rename(columns={col: col.removeprefix("Dest") for col in cache_upd.columns}).set_index(ColNames.FILE_ID)
    register.update(cache_upd)
    metadata.update(cache_upd[[ColNames.FILE_PATH]])

    # Save cache
    register.save(dropna=False)
    metadata.save(dropna=True)

    # Save summary
    save_op_report(files_df, timestamp)

###############################
####### FUNCTIONALITY #########
###############################

def reorganise(
        dir_path: str,
        config: Config,
        clear_cache: bool = False
     )-> None:
    return organise(
        src_roots=[dir_path],
        dest_root=dir_path,
        op_name="move",
        config=config,
        clear_cache=clear_cache
    )

def restore(
        run_id: str,
        op_name: str,
        config: Config,
    ) -> None:

    print(f"{op_name.upper()} FILES")

    # Record start timestamp in nanoseconds
    timestamp = time.time_ns()
    
    # Validate that the requested operation is supported
    ensure_valid(op_name, OPERATIONS)

    # Load cache
    register, metadata = config.register, config.metadata

    # if not os.path.exists(data_path):
    #     raise FileNotFoundError(f"Report not found: {data_path}")

    move_history = load_json("cache\\move_history.json")
    loaded_history = move_history.get(run_id, {})

    ensure_not_empty(loaded_history, name="files")    

    files_df = pd.DataFrame().from_dict(loaded_history, orient="index")
    files_df = files_df.rename(columns={col:dest(col) for col in files_df.columns})
    files_df = files_df.join(register.data, how="left")
    files_df = files_df.reset_index(names=[ColNames.FILE_ID])

    # Run operation pipeline
    run_op_pipeline(files_df, register, metadata, op_name, timestamp)

def organise(
        src_roots: list[str],
        dest_root: str,
        op_name: str,
        config: Config,
        file_categories: None | list[str] = None,
        clear_cache: bool = False,
    ) -> None:
    
    # implement category consistency check
    
    print(f"{op_name.upper()} FILES")

    # Record start timestamp in nanoseconds
    timestamp = time.time_ns()
    
    # Validate that the requested operation is supported
    ensure_valid(op_name, OPERATIONS)

    # Normalize incoming passes
    src_roots = list({os.path.normpath(src_root) for src_root in src_roots})
    dest_root = os.path.normpath(dest_root)

    # Load cache
    register, metadata = config.register, config.metadata
    # Clear cache tables if requested
    if clear_cache:
        metadata.clear()
        register.clear()
    # Load external service
    exif = config.exif
    # Load dir schema
    dir_schema = config.dir_schema

    # Validate and select source roots
    root_locs = inspect_directories(src_roots)

    if len(src_roots) == 1 and src_roots[0] == dest_root:
        processing_configs = {path: DirProcessingConfig(start_depth=0, target_depth=dir_loc.depth) for path, dir_loc in root_locs.items()}
    else:
        processing_configs = build_processing_configs(root_locs)

    # Extract files to process
    # print(f"{repeat('_', tbl1_w)}")
    search_summary = []
    file_records = []
    total_files = 0
    for path, path_config in processing_configs.items():
        n_files = 0
        for depth, dirpath, filenames in iter_dir_tree(path, path_config.start_depth, path_config.target_depth):
            for filename in filenames:
                file_records.append((path, path_config.target_depth, dirpath, depth, filename))
                n_files += 1
        total_files += n_files
        search_summary.append(search_count(desc="Files found",  path=path, n=n_files))
    print("\n".join(search_summary))
    # search_status(desc="Total", n=total_files, cell_widths=tbl1_col_w)
    ensure_not_empty(file_records, name="files")

    # Pre-processing
    files_df = pd.DataFrame(file_records, columns=[ColNames.ROOT, ColNames.ROOT_PROCESSING_DEPTH, ColNames.FILE_DIR_PATH, ColNames.FILE_DIR_DEPTH, ColNames.FILE_NAME])
    files_df[ColNames.EXIF_ARGS] = "".join(EXIFTOOL_ARGS)
    files_df[dest(ColNames.ROOT)] = dest_root
    files_df = add_file_path([ColNames.FILE_DIR_PATH, ColNames.FILE_NAME], ColNames.FILE_PATH).run(files_df)
    files_df = add_os_stat(
        src_col=dest(ColNames.ROOT),
        stat_to_dest_cols = {
            "st_dev": dest(ColNames.INODE_DEV),
        }
    ).run(files_df)
    files_df = add_os_stat(
        src_col=ColNames.FILE_PATH,
        stat_to_dest_cols={
            "st_dev": ColNames.INODE_DEV,
            "st_ino": ColNames.INODE,
            "st_size": ColNames.SIZE,
            "st_mtime": ColNames.MODIFIED_AT,
        }
    ).run(files_df)
    files_df = add_file_id([ColNames.INODE_DEV, ColNames.INODE], ColNames.FILE_ID).run(files_df)
    files_df = files_df.set_index(ColNames.FILE_ID, drop=False)
    
    # Identify new and changed files
    in_cache = register.data.loc[register.data.index.intersection(files_df.index)]
    date_match = files_df[ColNames.MODIFIED_AT].eq(in_cache[ColNames.MODIFIED_AT])
    size_match = files_df[ColNames.SIZE].eq(in_cache[ColNames.SIZE])
    args_match = files_df[ColNames.EXIF_ARGS].eq(in_cache[ColNames.EXIF_ARGS])
    found = date_match | size_match | args_match # True for cached and unchanged
    print(bool_summary(found, desc="Check cache"))

    if not found.all():

        # Extract exif metadata
        files_to_exif = files_df.loc[~found, ColNames.FILE_PATH].to_list()
        exif_results = list(tqdm(exif.extract(files_to_exif, args=EXIFTOOL_ARGS), total=len(files_to_exif), desc="Extract EXIF", bar_format=tqdm_bar()))
        exif_df = pd.DataFrame(exif_results).rename(columns={"SourceFile":ColNames.FILE_PATH})
        exif_df[ColNames.FILE_PATH] = exif_df[ColNames.FILE_PATH].apply(os.path.normpath)
        exif_df = exif_df.merge(files_df[[ColNames.FILE_ID, ColNames.FILE_PATH]], how="left", on=ColNames.FILE_PATH).set_index(ColNames.FILE_ID)
        print(bool_summary(exif_df[ColNames.FILE_PATH].notna()))
        
        # Update cache
        new = exif_df.index.difference(in_cache.index)
        updated = exif_df.index.intersection(in_cache.index)
        register.add(files_df.loc[new, register.data.columns])
        metadata.add(exif_df.loc[new])
        register.update(files_df.loc[updated, register.data.columns]) # ensure dtype alignment
        metadata.update(exif_df.loc[updated]) # ensure dtype alignment

    # Enrich files with exif metadata
    metadata_cols = metadata_filter.select(metadata.data.columns)
    files_df = files_df.merge(metadata.data[metadata_cols], how="left", left_index=True, right_index=True)
    
    # Get file categories from ref
    files_df = consolidate_file_ext([ColNames.FILE_TYPE_EXT, ColNames.FILE_NAME], ColNames.CONSOLIDATED_EXT).run(files_df)
    files_df = files_df.merge(config.ref[[ColNames.FILE_EXT, ColNames.FILE_CATEGORY]], how="left", left_on=ColNames.CONSOLIDATED_EXT, right_on=ColNames.FILE_EXT)
    files_df[ColNames.FILE_CATEGORY] = files_df[ColNames.FILE_CATEGORY].fillna("Other")
    category_mask = (Condition(ColNames.FILE_CATEGORY, "isin", file_categories) if file_categories else AllRows()).apply(files_df)
    files_df = files_df.loc[category_mask]
    print(bool_summary(category_mask, desc="Filter category", label={True: "selected", False: "filtered"}))
    ensure_not_empty(files_df, name="files")

    # Resolve dest dir schema
    for cat in files_df[ColNames.FILE_CATEGORY].unique():
        enabled_dims = [dim for dim, enabled in dir_schema.get(cat, {}).items() if enabled]
        category_cond = Condition(ColNames.FILE_CATEGORY, "eq", cat)
        for dim in enabled_dims:
            if dim in CALCULATED_DIMENSIONS:
                if dim == label(dup(ColNames.FILE_HASH)):
                    dup_size = dup(ColNames.SIZE)
                    dup_hash = dup(ColNames.FILE_HASH)

                    is_dup_size = And([category_cond, Condition(dup_size, "eq", True)])
                    is_dup_hash = And([category_cond, Condition(dup_hash, "eq", True)])
                    is_unique_hash = And([category_cond, Condition(dup_hash, "ne", True)])
                    label_to_cond = {"dup": is_dup_hash, "unique": is_unique_hash}

                    files_df = add_duplicate_check(ColNames.SIZE, dup_size, category_cond, keep=False).run(files_df)
                    files_df = add_file_hash(ColNames.FILE_PATH, ColNames.FILE_HASH, is_dup_size).run(files_df)
                    files_df = add_duplicate_check(ColNames.FILE_HASH, dup_hash, category_cond, keep="first").run(files_df)
                    files_df = add_labels(dim, label_to_cond).run(files_df)
                elif dim == ColNames.EARLIEST_YEAR:
                    date_cols = date_filter.select(files_df.columns)
                    files_df = add_earliest_year(date_cols, dim, category_cond, DateParser()).run(files_df)
                elif dim == ColNames.IMAGE_COUNTRY:
                    files_df = add_image_country([ColNames.EXIF_GPS_LATITUDE, ColNames.EXIF_GPS_LONGITUDE], dim, category_cond, RGeocoder(mode=1, verbose=False)).run(files_df)
                elif dim == ColNames.WORKSHEETS_COUNT:
                    files_df = add_n_worksheets([ColNames.XML_HEADING_PAIRS], dim, category_cond).run(files_df)
                else:
                    raise ValueError(f"Calculation logic is absent for {dim}")
            else:
                if dim not in files_df.columns:
                    # print(f"Dimension {dim} not available")
                    continue
        requested_dims = DEFAULT_DIMENSIONS + enabled_dims
        files_df = assemble_dest_dir(requested_dims, category_cond).run(files_df)

    # Assemble dest file path
    files_df = add_unique_filename(ColNames.FILE_NAME, dest(ColNames.FILE_NAME)).run(files_df)
    files_df = add_file_path([dest(ColNames.FILE_DIR_PATH), dest(ColNames.FILE_NAME)], dest(ColNames.FILE_PATH)).run(files_df)

    # Run operation pipeline
    run_op_pipeline(files_df, register, metadata, op_name, timestamp)

###############################
############# MAIN ############
###############################

def main():
    parser = argparse.ArgumentParser(description="File organiser")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Select mode")

    # load ref data
    ref_data = pd.read_csv(EXTENSION_MAP_PATH, encoding="cp852") # review  encoding issue
    file_categories = sorted(ref_data[ColNames.FILE_CATEGORY].unique())

    # organise command
    org = subparsers.add_parser("organise")
    org.add_argument("--op", choices=OPERATIONS, default="copy")
    org.add_argument("--src", required=True, nargs="+", help="Source directories")
    org.add_argument("--dest", required=True, help="Destination directory")
    org.add_argument("--cat", choices=file_categories, nargs="+", default=None, help="Select file category")
    org.add_argument("--clear", action="store_true", help="Clear cache tables")

    # reorganise command
    reorg = subparsers.add_parser("reorganise")
    reorg.add_argument("--dir_path", required=True, help="Directory to reorganise in place")
    reorg.add_argument("--clear", action="store_true", help="Clear cache tables")

    # restore command
    res = subparsers.add_parser("restore")
    res.add_argument("--op", choices=OPERATIONS, default="copy")
    res.add_argument("--run_id", help="Data for restore")
    res.add_argument("--last", action="store_true", help="Restore from last run")

    args = parser.parse_args()

    # prepare config
    json_loader = JSONLoader(orient="index")
    json_writer = JSONWriter(orient="index", force_ascii=False)

    config = Config(
        register=Cache(path=REGISTER_PATH, writer=json_writer, loader=json_loader, schema=REGISTRY_SCHEMA),
        metadata=Cache(path=METADATA_PATH, writer=json_writer, loader=json_loader, schema=None),
        ref=ref_data,
        exif=Exif(path=EXIFTOOL_PATH, encoding=EXIFTOOL_ENCODING, batch_size=EXIFTOOL_BATCH_SIZE),
        csv_writer=CSVWriter(encoding="utf-8-sig"),
        dir_schema=load_json(DIR_SCHEMA_PATH),
    )

    if args.command == "organise":
       organise(
            src_roots=args.src,
            dest_root=args.dest,
            op_name=args.op,
            config=config,
            file_categories=args.cat,
            clear_cache=args.clear,
        )
    elif args.command == "reorganise":
        reorganise(
            dir_path=args.dir_path,
            config=config,
            clear_cache=args.clear,
        )
    elif args.command == "restore":
        restore(
            run_id=latest_run_id() if args.last else args.run_id,
            op_name=args.op,
            config=config
        )
    else:
        print(args.command)

if __name__ == "__main__":
    main()
