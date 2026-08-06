import pandas as pd
from enum import StrEnum, auto
from core.pipelines import prepare_dirs, add_depth_metrics, add_file_path, add_files_stat, add_cache_key, tag_columns, select_columns, resolve_extension, select_extension, normalize_categories, label_duplicates, extract_year, extract_country, extract_worksheets_count, assemble_dest_dir
from cli.tokens import Icon, Separator
from cli.components import Info, Prompt
from core.transformation import DateParser
from core.config import Config, Cache, Exif, Reference
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
from typing import Literal, Callable
from utils.path import iter_dir_hierarchy, is_parent, depth_from_dir, components_count
from utils.text import uppercase_text

load_dotenv()

#########        TO DO LIST      #########
# [info] with shutil.copy2 atime and ctime updated, mtime preserved
# [info] CacheKey blends inodedev, inode
# [scan_directories] instead of os.walk(), create recursion based on os.scandir()
# [scan_directories] supply dir and files container externally
# [df] rename Predicate class into RowMask or RowFilter, remove where from Compute and Transform
# [df] develop partial hash function
# [return] include missing files report
# i need a session id ???

TQDM_BAR = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
EXIFTOOL_ENV_VAR = "EXIF_PATH"
EXIFTOOL_EXECUTABLE = "exiftool"
STRUCTURE_ALIASES = {
    "DuplicateLabel": "DuplicateLabel",
    "FileCategory": "category",
    "CreationYear": "Year",
    "FileExtension": "File:FileTypeExtension",
    "CameraModel": "EXIF:Model",
    "ImageCountry": "Country",
    "WorksheetCount": "CountWorksheets"
}

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

def select_processing_targets(df: pd.DataFrame) -> pd.DataFrame: # dependency: set_processing_depth()
    
    # pandas does not store Python int natively, so the only way to extract int is to call .item() on np.intXX(a) stored in pandas
    # map, apply in the DF Processor returns DF with irrelevant Col name that i reassign to relevant. Potential issues with dtypes
    
    # Sort values from highest to lowest level dirs
    df = df.sort_values("DirDepth", ascending=True)
    
    pending = list(df.index)
    skipped = set()
    for pos, row_id in enumerate(pending):
        if row_id in skipped:
            continue
        dir_path = df.loc[row_id, "DirPath"]
        branch_depth = df.loc[row_id, "SubtreeDepth"].item()
        # CLI element
        print("\n".join([Separator.DASH.repeat(100), Info.ELEMENTS["processing"].generate(dir_path=dir_path), Icon.DOWNARROW.repeat(3)]))
        # Get user input on required processing depth
        processing_depth, in_action = set_processing_depth(branch_depth)
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
            pending_child = df.at[next_row_id, "DirPath"]
            if is_parent(dir_path, pending_child):
                child_depth = depth_from_dir(pending_child, dir_path)
                if child_depth <= processing_depth:
                    # CLI element
                    print("\n".join([Separator.DASH.repeat(100), Info.ELEMENTS["skipped"].generate(path=pending_child)]))
                    skipped.add(next_row_id)

    return df.loc[df["IsSelected"]==True, ["DirPath", "ProcessingDepth"]]

def scan_directories(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    
    files_data = {
        "SrcRoot": [],
        "ProcessingDepth": [],
        "FileName": [],
        "FileDir" : [],
        "FileDirDepth": [],
    }

    dirs_data = {
        "SrcRoot": [],
        "Dir": [], 
        "DirDepth": []
    }

    for row_id in df.index:
        input_dir = df.loc[row_id, "DirPath"]
        processing_depth = df.loc[row_id, "ProcessingDepth"]
        for depth, dir, filenames in iter_dir_hierarchy(input_dir, processing_depth):
            dirs_data["SrcRoot"].append(input_dir)
            dirs_data["Dir"].append(dir)
            dirs_data["DirDepth"].append(depth)
            for filename in filenames:
                files_data["SrcRoot"].append(input_dir)
                files_data["ProcessingDepth"].append(processing_depth)
                files_data["FileName"].append(filename)
                files_data["FileDir"].append(dir)
                files_data["FileDirDepth"].append(depth)
    
    files = pd.DataFrame(files_data)
    dirs = pd.DataFrame(dirs_data)
    
    if files.empty:
        raise ValueError("No files to process")
    
    return files, dirs

def remove_emptied_dirs(dirs_df: pd.DataFrame) -> pd.DataFrame:
    if dirs_df.empty:
        return dirs_df

    dirs_df = dirs_df.loc[dirs_df["DirDepth"] > 0].sort_values(by="DirDepth", ascending=False)
    dirs = dirs_df["Dir"].to_list()
    for dir in dirs:
        try:
            content = os.listdir(dir)
            if not content:
                os.rmdir(dir)
                dirs_df.loc[dirs_df["Dir"] == dir, "Status"] = f"DELETED"
                continue
            dirs_df.loc[dirs_df["Dir"] == dir, "Status"] = f"ERROR - Content available {content}"
        except Exception as e:
            dirs_df.loc[dirs_df["Dir"] == dir, "Status"] = f"ERROR - {e}"
    return dirs_df

def move(src_path, dest_path):
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.move(src_path, dest_path)
        if os.path.exists(src_path):
            raise RuntimeError("Source still exists")
    except Exception as e:
        return e

def copy(src_path, dest_path):
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        shutil.copy2(src_path, dest_path)
    except Exception as e:
        return e

###############################
####### MAIN FUNCTIONS ########
###############################

def restore(report_path: str, operation: Callable, config: Config) -> pd.DataFrame:

    if operation.__name__ not in ("copy", "move"):
        raise ValueError(f"Unknown operation: {operation.__name__}")

    if operation.__name__ == "move":
        print("MOVE operation selected — original files at the source will be permanently deleted after being moved to the destination")
        response = input("Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()
    
    register_df = config.register.load()
    data_df = config.data.load()

    report_df = pd.read_csv(report_path)
    restore_plan = report_df[["DestCacheKey", "DestFilePath", "FilePath"]]
    restore_plan = restore_plan.rename(columns={
        "DestCacheKey":"CacheKey",
        "DestFilePath":"FilePath",
        "FilePath":"DestFilePath"
    })

    if not restore_plan.empty:
        # execute operation
        tqdm_bar_desc = f"{operation.__name__} files into new structure"
        tqdm.pandas(desc=f"{tqdm_bar_desc:<40}", bar_format=TQDM_BAR)
        restore_plan[operation.__name__] = restore_plan.progress_apply(lambda row: operation(row["FilePath"], row["DestFilePath"]), axis=1)
        # post processing
        failed = restore_plan.loc[~restore_plan[operation.__name__].isna()] # accumulate in missing results
        completed = restore_plan.loc[restore_plan[operation.__name__].isna()]
        completed = add_files_stat(prefix="Dest").execute(completed)
        completed = add_cache_key(prefix="Dest").execute(completed)

        if not register_df.empty and data_df.empty:
            no_chg =  completed.loc[completed["CacheKey"] == completed["DestCacheKey"]].set_index("CacheKey")
            chg =  completed.loc[completed["CacheKey"] != completed["DestCacheKey"]].set_index("CacheKey")

            if not no_chg.empty:
                # change FilePath for CacheKey
                register_df.loc[no_chg.index, "FilePath"] = no_chg["DestFilePath"]
                data_df.loc[no_chg.index, "FilePath"] = no_chg["DestFilePath"]

            if not chg.empty:

                # copy data under DestCacheKey
                register_upd = register_df[register_df.index.isin(chg.index)]
                register_upd = register_upd.merge(chg[["DestCacheKey", "DestFilePath"]], left_index=True, right_index=True)
                register_upd = register_upd.set_index("DestCacheKey")
                register_upd = register_upd.drop(columns=["CacheKey", "FilePath"])
                register_upd = register_upd.rename(columns={"DestFilePath":"FilePath"})
                register_df = pd.concat([register_df, register_upd])

                data_upd = data_df[data_df.index.isin(chg["CacheKey"])]
                data_upd = data_upd.merge(chg[["DestCacheKey", "DestFilePath"]], left_index=True, right_index=True)
                data_upd = data_upd.set_index("DestCacheKey")
                data_upd = data_upd.drop(columns=["CacheKey", "FilePath"])
                data_upd = data_upd.rename(columns={"DestFilePath":"FilePath"})
                data_df = pd.concat([data_df, data_upd])

                if operation.__name__ == "move":
                    # drop stale cache entries
                    register_df = register_df.drop(chg.index, axis="index")
                    data_df = data_df.drop(chg.index, axis="index")

            # save cache tables
            config.register.save(register_df)
            config.data.save(data_df)

        # remove emptied dirs
        if operation.__name__ == "move":
            dirs_df = remove_emptied_dirs(dirs_df)
        
        return completed

    else:
        print("Nothing to restore")
        return  pd.DataFrame()

def organise(src_roots: str | list[str], dest_root: str, dest_structure: list[str], operation: Callable, config: Config) -> pd.DataFrame:

    if operation.__name__ not in ("copy", "move"):
        raise ValueError(f"Unknown operation: {operation.__name__}")

    if operation.__name__ == "move":
        print("MOVE operation selected — original files at the source will be permanently deleted after being moved to the destination")
        response = input("Proceed? [y/N]: ").strip().lower()
        if response == "n":
            return pd.DataFrame()

    # map dest structure features
    dest_structure = [STRUCTURE_ALIASES[name] for name in dest_structure if name in STRUCTURE_ALIASES]

    # Load configs
    register_df = config.register.load()
    data_df = config.data.load()
    ref_df = config.ref.load().rename(uppercase_text, axis="index")
    ctx = config.context

    # Validate and select source roots
    src_roots_df = pd.DataFrame(
        {
            "DirPath": [src_roots] if isinstance(src_roots, str) else src_roots,
            "IsInvalid": False,
            "IsDuplicate": False,
            "IsSelected": False
        }
    )
    src_roots_df = prepare_dirs().execute(src_roots_df)
    src_roots_df = add_depth_metrics().execute(src_roots_df)
    selected_roots_df = select_processing_targets(src_roots_df)

    # Extract files to process
    files_df, dirs_df = scan_directories(selected_roots_df)

    # Pre-processing
    files_df["ExifArgs"] = "".join(config.exif.args)
    files_df = add_file_path(prefix="").execute(files_df)
    files_df = add_files_stat(prefix="").execute(files_df)
    files_df = add_cache_key(prefix="").execute(files_df)

    new_files_df = files_df[~files_df["CacheKey"].isin(register_df.index)].set_index("CacheKey")
    known_files_df = files_df[files_df["CacheKey"].isin(register_df.index)].set_index("CacheKey")

    if not known_files_df.empty:

        date_change = register_df.loc[known_files_df.index, "ModifiedAt"] != known_files_df["ModifiedAt"] # risky check for float type
        size_change = register_df.loc[known_files_df.index, "Size"] != known_files_df["Size"]
        args_change = register_df.loc[known_files_df.index, "ExifArgs"] != known_files_df["ExifArgs"]
        changed_files_df = known_files_df.loc[date_change | size_change | args_change]

        if not changed_files_df.empty:
            changed_files = changed_files_df["FilePath"].to_list()
            exif_results = list(tqdm(config.exif.extract(changed_files), total=len(changed_files), desc=f"{"Extracting exif metadata (changed)":<40}", bar_format=TQDM_BAR))
            exif_df = pd.DataFrame(exif_results)
            exif_df["SourceFile"] = exif_df["SourceFile"].apply(os.path.normpath)
            exif_df = exif_df.rename(columns={"SourceFile": "FilePath"})
            exif_df = exif_df.merge(files_df[["FilePath", "CacheKey"]], how="left", on="FilePath")
            exif_df = exif_df.set_index("CacheKey")
            # update cache
            register_df.update(changed_files_df[["FilePath", "ModifiedAt", "Size", "ExifArgs"]]) # index based, so "CacheKey" should be index
            data_df.update(exif_df) # index based, so "CacheKey" should be index

    if not new_files_df.empty:
        new_files = new_files_df["FilePath"].to_list()
        exif_results = list(tqdm(config.exif.extract(new_files), total=len(new_files), desc=f"{"Extracting exif metadata (new)":<40}", bar_format=TQDM_BAR))
        exif_df = pd.DataFrame(exif_results)
        exif_df["SourceFile"] = exif_df["SourceFile"].apply(os.path.normpath)
        exif_df = exif_df.rename(columns={"SourceFile": "FilePath"})
        exif_df = exif_df.merge(files_df[["FilePath", "CacheKey"]], how="left", on="FilePath")
        exif_df = exif_df.set_index("CacheKey")
        # append cache
        register_df = pd.concat([register_df, new_files_df[["FilePath", "ModifiedAt", "Size", "ExifArgs"]]]) # index based, so "CacheKey" should be index
        data_df = pd.concat([data_df, exif_df]) # index based, so "CacheKey" should be index

    # assemple dest dir path
    metadata_df = data_df[data_df.index.isin(files_df["CacheKey"])]
    metadata_df = tag_columns(ctx).execute(metadata_df)
    metadata_df = select_columns(ctx).execute(metadata_df)
    metadata_df = resolve_extension(ctx).execute(metadata_df)
    metadata_df = select_extension(ctx).execute(metadata_df)
    metadata_df = metadata_df.merge(ref_df, how="left", left_on="Extension", right_index=True)
    metadata_df = normalize_categories(ctx).execute(metadata_df)
    metadata_df = label_duplicates(ctx).execute(metadata_df)
    metadata_df = extract_year(ctx).execute(metadata_df)
    metadata_df = extract_country(ctx).execute(metadata_df)
    metadata_df = extract_worksheets_count(ctx).execute(metadata_df)
    metadata_df = assemble_dest_dir(ctx, dest_root, dest_structure).execute(metadata_df)

    # drop files without metadata
    has_metadata = files_df.loc[files_df["CacheKey"].isin(metadata_df.index)]
    no_metadata = files_df.loc[~files_df["CacheKey"].isin(metadata_df.index)] # accumulate missing

    # prepare data to execute operation
    operation_plan = has_metadata[["CacheKey", "FilePath", "FileDir", "FileName"]].merge(metadata_df[["DestFileDir"]], how="left", left_on="CacheKey", right_index=True)
    operation_plan["DestFilePath"] = operation_plan.apply(lambda row: os.path.join(row["DestFileDir"], row["FileName"]), axis=1)
    operation_plan["IsDuplicate"] = operation_plan["DestFilePath"].duplicated()
    operation_plan["IsEqual"] = operation_plan["DestFilePath"] == operation_plan["FilePath"]
    operation_plan = operation_plan.loc[~operation_plan["IsDuplicate"] & ~operation_plan["IsEqual"]]

    if not operation_plan.empty:
        # execute operation
        tqdm_bar_desc = f"{operation.__name__} files into new structure"
        tqdm.pandas(desc=f"{tqdm_bar_desc:<40}", bar_format=TQDM_BAR)
        operation_plan[operation.__name__] = operation_plan.progress_apply(lambda row: operation(row["FilePath"], row["DestFilePath"]), axis=1)
        # post processing
        failed = operation_plan.loc[~operation_plan[operation.__name__].isna()] # accumulate missing
        completed = operation_plan.loc[operation_plan[operation.__name__].isna()]
        completed = add_files_stat(prefix="Dest").execute(completed)
        completed = add_cache_key(prefix="Dest").execute(completed)

        if not register_df.empty and data_df.empty:

            no_chg =  completed.loc[completed["CacheKey"] == completed["DestCacheKey"]].set_index("CacheKey")
            chg =  completed.loc[completed["CacheKey"] != completed["DestCacheKey"]].set_index("CacheKey")

            if not no_chg.empty:
                # change FilePath for CacheKey
                register_df.loc[no_chg.index, "FilePath"] = no_chg["DestFilePath"]
                data_df.loc[no_chg.index, "FilePath"] = no_chg["DestFilePath"]

            if not chg.empty:

                # copy data under DestCacheKey
                register_upd = register_df[register_df.index.isin(chg.index)]
                register_upd = register_upd.merge(chg[["DestCacheKey", "DestFilePath"]], left_index=True, right_index=True)
                register_upd = register_upd.reset_index().set_index("DestCacheKey")
                register_upd = register_upd.drop(columns=["index", "FilePath"])
                register_upd = register_upd.rename(columns={"DestFilePath":"FilePath"})
                register_df = pd.concat([register_df, register_upd])

                data_upd = data_df[data_df.index.isin(chg.index)]
                data_upd = data_upd.merge(chg[["DestCacheKey", "DestFilePath"]], left_index=True, right_index=True)
                data_upd = data_upd.reset_index().set_index("DestCacheKey")
                data_upd = data_upd.drop(columns=["index", "FilePath"])
                data_upd = data_upd.rename(columns={"DestFilePath":"FilePath"})
                data_df = pd.concat([data_df, data_upd])

                if operation.__name__ == "move":
                    # drop stale cache entries
                    register_df = register_df.drop(chg.index, axis="index")
                    data_df = data_df.drop(chg.index, axis="index")

            # save cache tables
            config.register.save(register_df)
            config.data.save(data_df)

        # remove emptied dirs
        if operation.__name__ == "move":
            dirs_df = remove_emptied_dirs(dirs_df)

        return completed

    else:
        print("Nothing to organize")
        return  pd.DataFrame()

if __name__ == "__main__":
    
    exif_path = find_exiftool()

    config = Config(
        register=Cache(
            path="cache/register.json",
            clear_cache=False,
            writer=JSONWriter(orient="index", indent=4, force_ascii=False, drop_na=False),
            loader=JSONLoader(orient="index")
        ),
        data=Cache(
            path="cache/data.json",
            clear_cache=False,
            writer=JSONWriter(orient="index", indent=4, force_ascii=False, drop_na=True),
            loader=JSONLoader(orient="index")
        ),
        ref=Reference(
            path="ref/extension.json",
            loader=JSONLoader(orient="index")
        ),
        exif=Exif(
            path=exif_path,
            batch_size=50,
            args=["-j", "-G", "-all", "--File:Directory"]
        ),
        context=Context(
            parser=DateParser(),
            geocoder=RGeocoder(mode=1, verbose=False)
        )
    )

    completed = organise(
        src_roots="D:\\MyOrganizedFiles_2",
        dest_root="D:\\MyOrganizedFiles_2",
        dest_structure=["DuplicateLabel", "FileCategory", "CreationYear", "CameraModel", "ImageCountry", "WorksheetCount"],
        operation=move,
        config=config
    )

    datestamp = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")
    CSVWriter(encoding="utf-8-sig").save(completed, f"output\\completed_{datestamp}.csv")

    # rollback_df = rollback("D:\\Development\\Software\\Projects\\file_organiser\\output\\summary_20260805T165005.csv", operation=copy, config=config)
    # datestamp = datetime.strftime(datetime.now(), "%Y%m%dT%H%M%S")
    # CSVWriter(encoding="utf-8-sig").save(rollback_df, f"output\\rollback_{datestamp}.csv")
