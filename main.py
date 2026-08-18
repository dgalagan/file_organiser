import pandas as pd
from enum import StrEnum, auto
from core.pipelines import dup_label_col, dest_col, prepare_dirs, add_depth_metrics, assemble_file_path, add_stat, tag_columns, select_columns, consolidate_file_ext, exclude_rows, assemble_dest_dir
from cli.tokens import Icon, Separator
from cli.components import Info, Prompt
from core.transformation import DateParser
from core.config import Config, Cache, Exif, Reference
from constants import TagsMapping, Tags, Cols
from dataframe.context import Context
from dataframe.write import CSVWriter, JSONWriter
from dataframe.load import JSONLoader
from dotenv import load_dotenv
from datetime import datetime
import os
import pandas as pd
from reverse_geocoder import RGeocoder
import shutil
from tqdm import tqdm
from typing import Callable
from utils.path import iter_dir_tree, is_parent, depth_from_dir
from utils.text import uppercase_text

load_dotenv()

###############################
############ TO-DO ############
###############################

# [info] with shutil.copy2 atime and ctime updated, mtime preserved
# [info] CacheKey blends inodedev, inode

# [scan_directories] instead of os.walk(), create recursion based on os.scandir()
# [scan_directories] supply dir and files container externally
# [df] rename Predicate class into RowMask or RowFilter, remove where from Compute and Transform
# [df] develop partial hash function
# [df] in Combined filter if selected empty return AllCols
# [config] add filter rows func into config

TQDM_BAR = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
EXIFTOOL_ENV_VAR = "EXIF_PATH"
EXIFTOOL_EXECUTABLE = "exiftool"
CACHE_DIR = "cache"
CACHE_METADATA = "metadata.json"
CACHE_REGISTER = "register.json"
REGISTER_COLS = [Cols.FILE_PATH, Cols.FILE_NAME, Cols.MODIFIED_AT, Cols.SIZE, Cols.EXIF_ARGS]

class MenuActions(StrEnum):
    EXIT = auto()
    INTERRUPT = auto()
    SKIP = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

def find_exiftool() -> str:
    path = os.environ.get(EXIFTOOL_ENV_VAR) or shutil.which(EXIFTOOL_EXECUTABLE)
    if not path:
        raise RuntimeError("ExifTool not found")
    return path

def set_processing_depth(branch_depth: int) -> tuple[int | None, StrEnum]: # dependency: select_processing_targets()
    
    if not isinstance(branch_depth, int) or isinstance(branch_depth, bool):
        raise TypeError(f"branch_depth must be an int, got {type(branch_depth).__name__}")

    if branch_depth < 0:
        raise ValueError(f"branch_depth must be non-negative, got {branch_depth}")

    depth_range = f"0-{branch_depth}" if branch_depth else "0"

    while True:
        try:
            depth_input = input(Prompt.ELEMENTS["depth"].generate(range=depth_range))
            if depth_input == "":
                print(f"Skipping this path")
                return None, MenuActions.SKIP
            depth_input = int(depth_input)
            if 0 <= depth_input <= branch_depth:
                return depth_input, MenuActions.SUCCESS
            print(Warning.ELEMENTS["invalid_input"].generate())
            continue

        except ValueError:
            print(Warning.ELEMENTS["invalid_input"].generate())
            continue

        except KeyboardInterrupt:
            print(f"Depth input interrupted")
            return None, MenuActions.INTERRUPT

def select_roots(df: pd.DataFrame) -> pd.DataFrame: # dependency: set_processing_depth()
    
    # pandas does not store Python int natively, so the only way to extract int is to call .item() on np.intXX(a) stored in pandas
    # map, apply in the DF Processor returns DF with irrelevant Col name that i reassign to relevant. Potential issues with dtypes
    
    # Sort values from highest to lowest level dirs
    df = df.sort_values("RootDepth", ascending=True)
    
    pending = list(df.index)
    skipped = set()
    for pos, row_id in enumerate(pending):
        if row_id in skipped:
            continue
        src_root = df.loc[row_id, "SrcRoot"]
        tree_depth = df.loc[row_id, "RootTreeDepth"].item()
        # CLI element
        print("\n".join([Separator.DASH.repeat(100), Info.ELEMENTS["processing"].generate(dir_path=src_root), Icon.DOWNARROW.repeat(3)]))
        # Get user input on required processing depth
        processing_depth, in_action = set_processing_depth(tree_depth)
        match in_action:
            case MenuActions.SKIP:
                continue
            case MenuActions.SUCCESS:
                df.loc[row_id, "IsSelected"] = True
                df.loc[row_id, "ProcessingDepth"] = processing_depth
            case MenuActions.INTERRUPT:
                # return partial selection
                break
        # Check if child exist next to the
        for next_row_id in pending[pos+1:]:
            if next_row_id in skipped:
                continue
            pending_child = df.at[next_row_id, "SrcRoot"]
            if is_parent(src_root, pending_child):
                child_depth = depth_from_dir(pending_child, src_root)
                if child_depth <= processing_depth:
                    # CLI element
                    print("\n".join([Separator.DASH.repeat(100), Info.ELEMENTS["skipped"].generate(path=pending_child)]))
                    skipped.add(next_row_id)

    return df.loc[df["IsSelected"]==True, ["SrcRoot", "ProcessingDepth"]]


def remove_dir(dir_path: str):
    try:
        content = os.listdir(dir_path)
        if not content:
            os.rmdir(dir_path)
            return None
        else:
            return f"ERROR - directory not empty {content}"
    except Exception as e:
        return f"ERROR - {e}"

def move(src_path: str, dest_path: str):
    try:
        if os.path.exists(dest_path):
            raise RuntimeError("Destination occupied")
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.move(src_path, dest_path)
        if os.path.exists(src_path):
            raise RuntimeError("Source still exists")
    except Exception as e:
        return e

def copy(src_path: str, dest_path: str):
    try:
        if os.path.exists(dest_path):
            raise RuntimeError("Destination occupied")
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.copy2(src_path, dest_path)
    except Exception as e:
        return e

###############################
####### MAIN FUNCTIONS ########
###############################

def restore(report_path: str, operation: Callable, config: Config) -> pd.DataFrame:

    if operation not in (copy, move):
        raise ValueError(f"Unknown operation: {operation.__name__}")

    if operation is move:
        print("MOVE operation selected — original files at the source will be permanently deleted after being moved to the destination")
        response = input("Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()

    # Load cache
    register, metadata = config.register, config.metadata

    for cache in (register, metadata):
        cache.load()

    files_df = pd.read_csv(report_path)[[dest_col(Cols.FILE_ID), dest_col(Cols.FILE_PATH), Cols.FILE_PATH]]
    # files_df = report_df[[dest_col(Cols.FILE_ID), dest_col(Cols.FILE_PATH), Cols.FILE_PATH]]
    files_df = files_df.rename(columns={
        dest_col(Cols.FILE_ID): Cols.FILE_ID,
        dest_col(Cols.FILE_PATH): Cols.FILE_PATH,
        Cols.FILE_PATH: dest_col(Cols.FILE_PATH)
    })

    if not files_df.empty:

        # Execute operation
        tqdm.pandas(desc=f"{f"{operation.__name__} files into new structure":<40}", bar_format=TQDM_BAR)
        files_df[operation.__name__] = files_df.progress_apply(lambda row: operation(row[Cols.FILE_PATH], row[dest_col(Cols.FILE_PATH)]), axis=1)
        files_df = add_stat(prefix="Dest", metrics=["dev", "ino", "id"]).execute(files_df)

        # Remove emptied dirs
        if operation is move:
            files_df = files_df.loc[files_df[Cols.FILE_DIR_DEPTH] > 0].sort_values(by=Cols.FILE_DIR_DEPTH, ascending=False)
            tqdm.pandas(desc=f"{f"remove empty directories":<40}", bar_format=TQDM_BAR)
            files_df["rmdir"] = files_df[Cols.FILE_DIR_PATH].progress_apply(lambda dir_path: remove_dir(dir_path))

        # Update cache
        completed = files_df.loc[files_df[operation.__name__].isna(), [Cols.FILE_ID, dest_col(Cols.FILE_ID), dest_col(Cols.FILE_PATH)]]
        completed = completed.rename(columns={dest_col(Cols.FILE_PATH): Cols.FILE_PATH})

        no_chg_id = completed.loc[completed[Cols.FILE_ID] == completed[dest_col(Cols.FILE_ID)]]
        no_chg_id = no_chg_id[[Cols.FILE_ID, Cols.FILE_PATH]].set_index(Cols.FILE_ID)

        chg_id = completed.loc[completed[Cols.FILE_ID] != completed[dest_col(Cols.FILE_ID)]]
        src_to_dest = dict(zip(chg_id[Cols.FILE_ID], chg_id[dest_col(Cols.FILE_ID)]))
        chg_id = chg_id[[dest_col(Cols.FILE_ID), Cols.FILE_PATH]].set_index(dest_col(Cols.FILE_ID))
        
        for cache in (register, metadata):
            if not no_chg_id.empty:
                cache.update(no_chg_id)
            if not chg_id.empty:
                cache.clone(src_to_dest)
                cache.update(chg_id)
                if operation is move:
                    # drop stale cache entries
                    cache.delete(chg_id[Cols.FILE_ID])

        # Save cache
        register.save(dropna=False)
        metadata.save(dropna=True)

        return files_df

    else:
        print("Nothing to restore")
        return files_df

def organise(src_roots: str | list[str], dest_root: str, dest_structure: list[str], operation: Callable, config: Config, clear_cache: bool = False) -> pd.DataFrame:

    if operation not in (copy, move):
        raise ValueError(f"Unknown operation: {operation.__name__}")

    if operation is move:
        print("MOVE operation selected — original files at the source will be permanently deleted after being moved to the destination")
        response = input("Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()

    # Load cache
    register, metadata = config.register, config.metadata

    for cache in (register, metadata):
        if clear_cache:
            cache.clear()
        else:
            cache.load()

    # Load ref
    ref_df = config.ref.load().rename(uppercase_text, axis="index").rename(columns={"category": Cols.FILE_CATEGORY})
    # Load context
    ctx = config.context

    # Validate and select source roots
    src_roots_df = pd.DataFrame(
        {
            "SrcRoot": [src_roots] if isinstance(src_roots, str) else src_roots,
            "IsInvalid": False,
            "IsDuplicate": False,
            "IsSelected": False
        }
    )
    src_roots_df = prepare_dirs().execute(src_roots_df)
    src_roots_df = add_depth_metrics().execute(src_roots_df)
    selected_roots_df = select_roots(src_roots_df)

    # Extract files to process
    dir_records = []
    file_records = []
    for row_id in selected_roots_df.index:
        src_root = selected_roots_df.loc[row_id, Cols.SRC_ROOT]
        processing_depth = selected_roots_df.loc[row_id, Cols.ROOT_PROCESSING_DEPTH]
        for depth, dir, filenames in iter_dir_tree(src_root, processing_depth):
            dir_records.append((src_root, processing_depth, dir, depth))
            for filename in filenames:
                file_records.append((src_root, processing_depth, dir, depth, filename))
    dirs_df = pd.DataFrame(dir_records, columns=[Cols.SRC_ROOT, Cols.ROOT_PROCESSING_DEPTH, Cols.DIR_PATH, Cols.DIR_DEPTH])
    files_df = pd.DataFrame(file_records, columns=[Cols.SRC_ROOT, Cols.ROOT_PROCESSING_DEPTH, Cols.FILE_DIR_PATH, Cols.FILE_DIR_DEPTH, Cols.FILE_NAME])

    # Pre-processing
    files_df[Cols.EXIF_ARGS] = "".join(config.exif.args)
    files_df = assemble_file_path(prefix="").execute(files_df)
    files_df = add_stat(prefix="", metrics=["size", "mtime", "dev", "ino", "id"]).execute(files_df)

    # Extract exif metadata
    new_files_df = files_df[~files_df[Cols.FILE_ID].isin(register.data.index)].set_index(Cols.FILE_ID)
    known_files_df = files_df[files_df[Cols.FILE_ID].isin(register.data.index)].set_index(Cols.FILE_ID)

    changed_files_df = None

    if not known_files_df.empty:
        date_change = register.data.loc[known_files_df.index, Cols.MODIFIED_AT] != known_files_df[Cols.MODIFIED_AT] # risky check for float type
        size_change = register.data.loc[known_files_df.index, Cols.SIZE] != known_files_df[Cols.SIZE]
        args_change = register.data.loc[known_files_df.index, Cols.EXIF_ARGS] != known_files_df[Cols.EXIF_ARGS]
        changed_files_df = known_files_df.loc[date_change | size_change | args_change]

    to_exif_df = pd.concat([new_files_df, changed_files_df])
    if not to_exif_df.empty:
        files_to_exif = to_exif_df[Cols.FILE_PATH].to_list()
        exif_results = list(tqdm(config.exif.extract(files_to_exif), total=len(files_to_exif), desc=f"{"Extracting exif metadata":<40}", bar_format=TQDM_BAR))
        exif_df = pd.DataFrame(exif_results)
        exif_df["SourceFile"] = exif_df["SourceFile"].apply(os.path.normpath)
        exif_df = exif_df.merge(to_exif_df.reset_index()[[Cols.FILE_PATH, Cols.FILE_ID]], how="left", left_on="SourceFile", right_on=Cols.FILE_PATH)
        exif_df = exif_df.drop(columns="SourceFile")
        exif_df = exif_df.set_index(Cols.FILE_ID)

    # Update cache
    if not changed_files_df.empty:
        register.update(changed_files_df[REGISTER_COLS])
        metadata.update(exif_df.loc[changed_files_df.index])

    if not new_files_df.empty:
        register.add(new_files_df[REGISTER_COLS])
        metadata.add(exif_df.loc[new_files_df.index])

    # Select exif metadata
    metadata_df = tag_columns(ctx, name_tags=TagsMapping.NAME, keyword_tags=TagsMapping.KEYWORD).execute(metadata.data)
    selected_metadata_df = select_columns(
        ctx,
        names=[Cols.FILE_TYPE_EXT, Cols.XML_HEADING_PAIRS, Cols.EXIF_GPS_LATITUDE, Cols.EXIF_GPS_LONGITUDE, Cols.EXIF_MODEL],
        tags=[Tags.CREATE_DT, Tags.ACCESS_DT, Tags.MODIFY_DT]
    ).execute(metadata_df)

    # Enrich files with exif metadata, assemble destination file path
    files_df = files_df.merge(selected_metadata_df, how="left", left_on=Cols.FILE_ID, right_index=True)
    files_df = consolidate_file_ext(ctx).execute(files_df)
    files_df = exclude_rows(ctx, col=Cols.CONSOLIDATED_EXT, values=["MRIMGX"]).execute(files_df)
    files_df = files_df.merge(ref_df[Cols.FILE_CATEGORY], how="left", left_on=Cols.CONSOLIDATED_EXT, right_index=True)
    files_df = assemble_dest_dir(ctx, dest_root, dest_structure).execute(files_df)
    files_df = assemble_file_path(prefix="Dest").execute(files_df)

    # Execute operation
    tqdm.pandas(desc=f"{f"{operation.__name__} files into new structure":<40}", bar_format=TQDM_BAR)
    files_df[operation.__name__] = files_df.progress_apply(lambda row: operation(row[Cols.FILE_PATH], row[dest_col(Cols.FILE_PATH)]), axis=1)
    files_df = add_stat(prefix="Dest", metrics=["dev", "ino", "id"]).execute(files_df)

    # Remove emptied dirs
    if operation is move:
        dirs_df = dirs_df.loc[dirs_df[Cols.DIR_DEPTH] > 0].sort_values(by=Cols.DIR_DEPTH, ascending=False)
        tqdm.pandas(desc=f"{f"remove empty directories":<40}", bar_format=TQDM_BAR)
        dirs_df["rmdir"] = dirs_df[Cols.DIR_PATH].progress_apply(lambda dir_path: remove_dir(dir_path))

    # Update cache
    completed = files_df.loc[files_df[operation.__name__].isna(), [Cols.FILE_ID, dest_col(Cols.FILE_ID), dest_col(Cols.FILE_PATH)]]
    completed = completed.rename(columns={dest_col(Cols.FILE_PATH): Cols.FILE_PATH})

    no_chg_id = completed.loc[completed[Cols.FILE_ID] == completed[dest_col(Cols.FILE_ID)]]
    no_chg_id = no_chg_id[[Cols.FILE_ID, Cols.FILE_PATH]].set_index(Cols.FILE_ID)

    chg_id = completed.loc[completed[Cols.FILE_ID] != completed[dest_col(Cols.FILE_ID)]]
    src_to_dest = dict(zip(chg_id[Cols.FILE_ID], chg_id[dest_col(Cols.FILE_ID)]))
    chg_id = chg_id[[dest_col(Cols.FILE_ID), Cols.FILE_PATH]].set_index(dest_col(Cols.FILE_ID))
    
    for cache in (register, metadata):
        if not no_chg_id.empty:
            cache.update(no_chg_id)
        if not chg_id.empty:
            cache.clone(src_to_dest)
            cache.update(chg_id)
            if operation is move:
                # drop stale cache entries
                cache.delete(chg_id[Cols.FILE_ID])

    # save cache
    register.save(dropna=False)
    metadata.save(dropna=True)

    return files_df

if __name__ == "__main__":
    
    exif_path = find_exiftool()

    project_root = os.path.dirname(os.path.abspath(__file__)) # __file__ does not exist in REPL, Jupyter, debugger
    cache_dir_path = os.path.join(project_root, CACHE_DIR)
    register_path = os.path.join(cache_dir_path, CACHE_REGISTER)
    metadata_path = os.path.join(cache_dir_path, CACHE_METADATA)

    json_loader = JSONLoader(orient="index")
    json_writer = JSONWriter(orient="index", indent=4, force_ascii=False)

    config = Config(
        register=Cache(path=register_path, writer=json_writer, loader=json_loader),
        metadata=Cache(path=metadata_path, writer=json_writer, loader=json_loader),
        ref=Reference(path="ref/extension.json", loader=json_loader),
        exif=Exif(path=exif_path, batch_size=50, args=["-j", "-G", "-all", "--File:Directory"]),
        context=Context(parser=DateParser(), geocoder=RGeocoder(mode=1, verbose=False))
    )

    organised = organise(
        # src_roots=["D:\\OneDrive"],
        src_roots=["D:\\MyOrganizedFiles"],
        dest_root="D:\\MyOrganizedFiles",
        dest_structure=[dup_label_col(Cols.FILE_HASH), Cols.EARLIEST_YEAR, Cols.FILE_CATEGORY, Cols.EXIF_MODEL, Cols.IMAGE_COUNTRY, Cols.WORKSHEETS_COUNT],
        operation=move,
        config=config,
        clear_cache=False,
    )

    datestamp = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")
    CSVWriter(encoding="utf-8-sig").save(organised, f"output\\completed_{datestamp}.csv")

    # rollback_df = rollback("D:\\Development\\Software\\Projects\\file_organiser\\output\\summary_20260805T165005.csv", operation=copy, config=config)
    # datestamp = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")
    # CSVWriter(encoding="utf-8-sig").save(rollback_df, f"output\\rollback_{datestamp}.csv")
