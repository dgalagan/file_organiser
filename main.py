from enum import StrEnum, auto
from core.pipelines import assemble_file_path, add_stat, add_file_id, consolidate_file_ext, prepare_dimensions_calc, assemble_dest_dir
from cli.components import Notifications, Warnings, Errors, Prompt, TQDMDesc
from core.parser import DateParser
from core.config import Config, Cache, Exif, Reference
from core.categories import Category, CategorySelection
from core.tagstore import TagStore
from constants import Tags, Cols, PROJECT_ROOT, OUTPUT_DIR_PATH, REGISTER_PATH, METADATA_PATH, EXTENSION_MAP_PATH
from dataclasses import dataclass
from dataframe.pipeline import FilterRows
from dataframe.col_filter import ColumnFilter, NameFilter, KeywordFilter, CombinedFilter
from dataframe.predicate import Condition, And
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
from utils.text import uppercase_text
from collections import defaultdict

###############################
############ TO-DO ############
###############################

# [info] with shutil.copy2 atime and ctime updated, mtime preserved
# [info] CacheKey blends inodedev, inode
# [info] dir_position: dir's depth from drive (global reference)
# [info] dir_depth: deepest layer below dir, where dir itself is 0

# [scan_directories] instead of os.walk(), create recursion based on os.scandir()
# [scan_directories] supply dir and files container externally
# [df] rename Predicate class into RowMask or RowFilter, remove where from Compute and Transform
# [df] develop partial hash function
# [df] in Combined filter if selected empty return AllCols

EXIFTOOL_PATH = "D:/Development/Software/Projects/file_organiser/bin/exif/exiftool(-k).exe"
EXIFTOOL_ARGS = ["-j", "-G", "-all", "--File:Directory"]
EXIFTOOL_ENCOODING = "utf-8"
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
class DirInfo:
    dir_path: str
    dir_position: int
    dir_depth: int

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
def validate_dirs(roots: list[str]) -> list[tuple[str, int, int]]:
    unique_roots = set(os.path.normpath(root) for root in roots)
    result = []
    for root in unique_roots:
        if not is_dir(root) or is_empty(root):
            continue
        dir_position = depth_from_drive(root) 
        dir_depth = tree_depth(root)
        result.append((root, dir_position, dir_depth))
    return result
# try while loop / stack approach
def show_dirs_tree(roots_data: list[tuple[str, int, int]], coverage_bar: dict[str: str] = None) -> None:
    # Containers
    dir_structure = []
    seen = []
    # Sort by path name
    sorted_data = sorted(roots_data, key=lambda root_data: root_data[0])

    # Execution
    for root, dir_position, dir_depth in sorted_data:
        parent = None
        parent_position = -1
        parent_count = 0
        for seen_root, seen_dir_position, _ in seen:
            if is_parent(seen_root, root):
                parent_count += 1
                if seen_dir_position > parent_position:
                    parent, parent_position = seen_root, seen_dir_position
        bar = coverage_bar.get(root, (dir_depth + 1) * "|")
        pad = " " * max(0, 20 - dir_depth + 1)
        if parent:
            rel_path = os.path.relpath(root, parent)
            dir_structure.append(f"{bar}{pad}{parent_count * '  '}|_{rel_path}")
        else:
            dir_structure.append(f"{bar}{pad}{root}")
        seen.append((root, dir_position, dir_depth))

    # Print tree
    print("\n".join(dir_structure))

def select_dirs(roots_data: list[tuple[str, int, int]]) -> list[tuple[str, int]]: # dependency: get_depth_input() 
    # Depth bar colors
    input_color =  "\033[1;38;5;34m"
    covered_color = "\033[1;38;5;157m"
    uncovered_color = "\033[90m"
    reset = "\033[0m"
    # Containers
    result = []
    seen = []
    depth_bar = {}
    # Sort by path name
    sorted_data = sorted(roots_data, key=lambda root_data: root_data[1]) # by dir depth

    print(f"\nInput tree")
    show_dirs_tree(roots_data, coverage_bar=depth_bar)

    print("\n".join(["\nSelect processing depth", f"{INDENT}[blank]  Skip", f"{INDENT}[Ctrl+C] Abort\n"]))
    interrupt = False
    for root, dir_position, dir_depth in sorted_data:
        # Check parents in seen 
        parent = None
        parent_position = -1
        parent_depth_input = -1
        for seen_root, seen_dir_position, seen_depth_input in seen:
            if is_parent(seen_root, root) and seen_dir_position > parent_position:
                parent, parent_position, parent_depth_input = seen_root, seen_dir_position, seen_depth_input
        # Estimate how many layers were covered by parent 
        covered_depth = -1
        if parent:
            parent_processing_depth = parent_position + parent_depth_input
            child_max_depth = dir_position + dir_depth
            if parent_processing_depth >= dir_position:
                if parent_processing_depth < child_max_depth:
                    covered_depth = parent_processing_depth - dir_position
                else:
                    covered = covered_color + "|" * (dir_depth + 1) + reset
                    depth_bar[root] = covered
                    continue
        # Generate bar for interrupt case 
        if interrupt:
            covered = covered_color + "|" * covered_depth + reset
            uncovered = uncovered_color + '|' * (dir_depth - covered_depth) + reset
            depth_bar[root] = covered + uncovered
            continue
        # Get user input
        depth_range = [covered_depth + 1, dir_depth]
        depth_input, in_action = get_depth_input(root, depth_range)

        covered = covered_color + "|" * (covered_depth + 1) + reset
        if depth_input >= 0:
            user = input_color + '|' * (depth_input - covered_depth) + reset
            uncovered = uncovered_color + '|' * (dir_depth - depth_input) + reset
        else:
            user = ''
            uncovered = uncovered_color + '|' * (dir_depth - covered_depth) + reset
        depth_bar[root] = covered + user + uncovered

        match in_action:
            case MenuActions.SKIP:
                continue
            case MenuActions.SUCCESS:
                result.append((root, covered_depth + 1, depth_input))
                seen.append((root, dir_position, depth_input))
            case MenuActions.INTERRUPT:
                interrupt = True

    if not result:
        raise ValueError(Errors.ELEMENTS["empty_input"].build(subject="src roots")) #--- Error ---

    print(f"\nOutput tree")
    show_dirs_tree(sorted_data, coverage_bar=depth_bar)
    
    return result

def get_depth_input(root: str, depth_range: list[int]) -> tuple[int, StrEnum]: # dependency: select_dirs()

    depth_level = f"{depth_range[0]}-{depth_range[1]}" if depth_range[0] != depth_range[1] else depth_range[0]

    while True:
        try:
            print(f"{INDENT}{Prompt.ELEMENTS["depth_input"].build(dir_path=root, num=depth_level)}")
            depth_input = input(f"{INDENT*5} \\__depth: ")
            if depth_input == "":
                return -1, MenuActions.SKIP
            depth_input = int(depth_input)
            if depth_range[0] <= depth_input <= depth_range[1]:
                return depth_input, MenuActions.SUCCESS
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

def execute_operation(files_df: pd.DataFrame, operation: Callable, register: Cache, metadata: Cache, command: str = None, tagstore: TagStore = None):

    op_name = operation.__name__
    csv_writer = CSVWriter()

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

    # Save summary
    files_df = files_df.dropna(axis="columns", how="all")
    datestamp = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")
    summary_path = os.path.join(OUTPUT_DIR_PATH, f"{command}_{datestamp}.csv")

    print("\nSaved")
    for result in [register.save(dropna=False), metadata.save(dropna=True), csv_writer.save(files_df, summary_path)]:
        rel_path = os.path.relpath(result.path, PROJECT_ROOT)
        if result.success:
            print(f"{INDENT}{Notifications.ELEMENTS["save_done"].build(path=rel_path)}") #--- Notification ---
        else:
            print(f"{INDENT}{Notifications.ELEMENTS["save_failed"].build(path=rel_path, reason=result.error)}") #--- Notification ---

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

    files_df = execute_operation(files_df, operation, register, metadata, command="restore")

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

    valid_ops  = (copy, move)
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

    # Load ref
    ref_df = config.ref.load().rename(uppercase_text, axis="index").rename(columns={"category": Cols.FILE_CATEGORY})

    # Load services
    exif = config.exif
    geocoder = config.geocoder
    date_parser = config.parser

    # Validate and select source roots
    roots_with_depth = validate_dirs(src_roots)
    roots_selected = select_dirs(roots_with_depth)

    # Extract files to process
    file_records = []
    for root, starting_depth, processing_depth in roots_selected:
        root_files = []
        for depth, dir, filenames in iter_dir_tree(root, starting_depth, processing_depth):
            for filename in filenames:
                root_files.append((root, processing_depth, dir, depth, filename))
        file_records.extend(root_files)

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

    # Check if there is enough space to procesfiles
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
    files_df = files_df.merge(ref_df[Cols.FILE_CATEGORY], how="left", left_on=Cols.CONSOLIDATED_EXT, right_index=True)
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
    files_df = execute_operation(files_df, operation, register, metadata, command="organise")

    return files_df

def main(command: str = "organise"):

    json_loader = JSONLoader(orient="index")
    json_writer = JSONWriter(orient="index", force_ascii=False)

    config = Config(
        register=Cache(path=REGISTER_PATH, writer=json_writer, loader=json_loader),
        metadata=Cache(path=METADATA_PATH, writer=json_writer, loader=json_loader),
        ref=Reference(path=EXTENSION_MAP_PATH, loader=json_loader),
        exif=Exif(path=EXIFTOOL_PATH, encoding=EXIFTOOL_ENCOODING, batch_size=EXIFTOOL_BATCH_SIZE),
        geocoder=RGeocoder(mode=1, verbose=False),
        parser=DateParser(),
    )

    all_categories = json_loader.load(EXTENSION_MAP_PATH).category.drop_duplicates().to_list()
    category_selection = CategorySelection(categories=all_categories).get()

    if command == "organise":
       organise(
            # src_roots=["D:\\HDD Data\\Ciklum", "D:\\OneDrive", "D:\\HDD Data", "D:\\OneDrive\\Desktop\\Books", "D:\\HDD Data\\Ciklum\\Adidas", "D:\\HDD Data\\CurriculumVitae", "D:\\HDD Data\\OTHER", "D:\\HDD Data\\OTHER\\Flashka 2\\ТПК2\\Презентации\\Рассылка на КОК"],
            # src_roots=["D:\\MyOrganizedFiles1"],
            src_roots = ["D:\\OneDrive"],
            dest_root="D:\\MyOrganizedFiles",
            operation=copy,
            config=config,
            file_categories=category_selection,
            dir_schema=DIR_SCHEMA,
            clear_cache=True,
        )

    elif command == "restore":
        restore(
            report_name="organise_20260831T182240.csv",
            operation=move,
            config=config
        )

if __name__ == "__main__":
    main()
