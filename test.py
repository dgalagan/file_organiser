import pandas as pd
from enum import StrEnum, auto
from pipelines import _preparation_pipeline, add_depth_metrics, add_file_path, add_path_exists, add_files_stat, add_cache_key, select_completed, select_failed, select_skipped, tag_columns, select_columns, resolve_extension, select_extension, normalize_categories, label_duplicates, extract_year, extract_country, extract_worksheets_count, assemble_dest_dir
from cli.tokens import Icon, Separator
from cli.components import Info, Prompt
from core.exiftool import find_exiftool, extract_exif_data
from core.transformation import DateParser
from dataframe.context import Context
from dataframe.writer import CSVWriter
from dotenv import load_dotenv
import os
import pandas as pd
from reverse_geocoder import RGeocoder
import shutil
from tqdm import tqdm
from utils.text import uppercase_text
from utils.json import save_json
from utils.path import iter_dir_hierarchy, is_parent, depth_from_dir
import hashlib

load_dotenv()

class MenuActions(StrEnum):
    EXIT = auto()
    INTERRUPT = auto()
    SKIP = auto()
    SUCCESS = auto()
    FAILED = auto()
    RESTART = auto()

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

def prepare_dirs(src: list[str]) -> pd.DataFrame:

    dir_data = pd.DataFrame({
        "DirPath": src,
        "IsInvalid": False,
        "IsDuplicate": False,
    })

    return _preparation_pipeline().execute(dir_data)

def select_processing_targets(df: pd.DataFrame) -> pd.DataFrame:
    
    # pandas does not store Python int natively, so the only way to extract int is to call .item() on np.intXX(a) stored in pandas
    # map, apply in the DF Processor returns DF with irrelevant Col name that i reassign to relevant. Potential issues with dtypes
    
    # Assign default values
    df["IsSelected"] = False
    
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
        "SrcDir": [],
        "ProcessingDepth": [],
        "FileName": [],
        "FileDir" : [],
        "FileDirDepth": [],
    }

    dirs_data = {
        "SrcDir": [],
        "Dir": [], 
        "DirDepth": []
    }

    for row_id in df.index:
        input_dir = df.loc[row_id, "DirPath"]
        processing_depth = df.loc[row_id, "ProcessingDepth"]
        for depth, dir, filenames in iter_dir_hierarchy(input_dir, processing_depth):
            dirs_data["SrcDir"].append(input_dir)
            dirs_data["Dir"].append(dir)
            dirs_data["DirDepth"].append(depth)
            for filename in filenames:
                files_data["SrcDir"].append(input_dir)
                files_data["ProcessingDepth"].append(processing_depth)
                files_data["FileName"].append(filename)
                files_data["FileDir"].append(dir)
                files_data["FileDirDepth"].append(depth)
    
    files = pd.DataFrame(files_data)
    dirs = pd.DataFrame(dirs_data)
    
    if files.empty:
        raise ValueError("No files to process")
    
    return files, dirs

def calc_cache_key(dev: str, ino: str, exif_args: list[str] | None = None) -> str:
    exif_args = exif_args or []
    parts = f"{dev}|{ino}|{"|".join(sorted(exif_args))}"
    return hashlib.md5(parts.encode()).hexdigest()

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

def move_files(execution_df: pd.DataFrame) -> pd.DataFrame:
    if execution_df.empty:
        return execution_df
    
    desc = "Move files into new structure"
    for row_id, row in tqdm(execution_df.iterrows(), total=len(execution_df), desc=f"{desc:<40}", bar_format='{l_bar}{bar:60}{r_bar}{bar:-10b}'):
        src_dir = row["FileDir"]
        dest_dir = row["DestFileDir"]
        filename = row["FileName"]
        try:
            src_path = os.path.join(src_dir, filename)
            dest_path = os.path.join(dest_dir, filename)
            
            if src_path == dest_path:
                execution_df.at[row_id, "Status"] = "SKIPPED"
                continue
            
            os.makedirs(dest_dir, exist_ok=True)
            shutil.move(src_path, dest_path)
            
            if os.path.exists(src_path):
                raise RuntimeError("Move incomplete - source still exists")

            execution_df.at[row_id, "Status"] = "COMPLETED"
        except Exception as e:
            execution_df.at[row_id, "Status"] = "FAILED"
            execution_df.at[row_id, "ErrorMessage"] = str(e)
    
    return execution_df

def copy_files(execution_df: pd.DataFrame) -> pd.DataFrame:
    if execution_df.empty:
        return execution_df
    
    # Check available space on the drive
    files_size = execution_df["Size"].sum()
    _, _, free = shutil.disk_usage(dest_root)
    if files_size >= free:
        raise ValueError(f"Not enough space to copy files: free {int(free /(1<<30))} GB, required {int(files_size /(1<<30))} GB")

    desc = "Copying files into new structure"
    for row_id, row in tqdm(execution_df.iterrows(), total=len(execution_df), desc=f"{desc:<40}", bar_format='{l_bar}{bar:60}{r_bar}{bar:-10b}'):
        src_dir = row["FileDir"]
        dest_dir = row["DestFileDir"]
        filename = row["FileName"]
        try:
            src_path = os.path.join(src_dir, filename)
            dest_path = os.path.join(dest_dir, filename)
            
            if src_path == dest_path:
                execution_df.at[row_id, "Status"] = "SKIPPED"
                continue

            if os.path.exists(dest_path):
                raise RuntimeError("Copy aborted - dest already exists")
            
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copy2(src_path, dest_path)
            execution_df.at[row_id, "Status"] = "COMPLETED"
        except Exception as e:
            execution_df.at[row_id, "Status"] = "FAILED"
            execution_df.at[row_id, "ErrorMessage"] = str(e)
    return execution_df

def rollback(files_df: pd.DataFrame) -> pd.DataFrame:
    return files_df

def df_to_dict(df: pd.DataFrame, *, drop_na: bool = False, orient: str ="index") -> dict:
    if drop_na:
        return {str(row_id): row.dropna().to_dict() for row_id, row in df.iterrows()}
    return df.to_dict(orient=orient)

# General
src_roots = ["D:\\MyOrganizedFiles"]
dest_root = "D:\\MyOrganizedFiles"

# Operation
action = "move"

# Exif
exif_params = ["-j", "-G"]
include_tags = ["-all"]
exclude_tags = ["--File:Directory"]
exif_args = exif_params + include_tags + exclude_tags
batch_size = 50

# Cache
clear_cache = False
registry_cache = "cache/register.json"
data_cache = "cache/data.json"
ref = "ref/extension.json"

# Path components selection
component_aliases = {
    "DuplicateLabel": "DuplicateLabel",
    "FileCategory": "category",
    "CreationYear": "Year",
    "FileExtension": "File:FileTypeExtension",
    "CameraModel": "EXIF:Model",
    "ImageCountry": "Country",
    "WorksheetCount": "CountWorksheets"
}

component_structure = {
    "General": ["DuplicateLabel", "FileCategory", "CreationYear", "FileExtension"],
    "Image": ["CameraModel", "ImageCountry"],
    "Data-Excel": ["WorksheetCount"]
}

def change_item_order(path_components: dict, group: str, component: str, new_idx: int) -> dict[str, list]:
    group_items = path_components[group]
    group_items.remove(component)
    group_items.insert(new_idx, component)
    return path_components

def remove_item(path_components: dict, group: str, component: str):
    group_items = path_components[group]
    group_items.remove(component)
    return path_components

def resolve_components(path_components: dict, aliases: dict) -> list:
    return [aliases[component] for components in path_components.values() for component in components]

reorder = [("General", "DuplicateLabel", 0)] # (group, component, new_position) -> ("General", "DuplicateLabel", 3)
remove = [("General", "FileExtension")] # (group, component) -> ("General", "DuplicateLabel")
if reorder:
    for instruction in reorder:
        component_structure = change_item_order(component_structure, *instruction)
if remove:
    for instruction in remove:
        component_structure = remove_item(component_structure, *instruction)
path_components = resolve_components(component_structure, component_aliases)

# load exif data
exif_path = find_exiftool()

# load cache
for cache_path in (registry_cache, data_cache):
    if clear_cache or not os.path.exists(cache_path):
        os.makedirs(os.path.dirname(cache_path))
        save_json(cache_path, {})

register_df = pd.read_json(registry_cache, orient="index")
data_df = pd.read_json(data_cache, orient="index")
ref_df = pd.read_json(ref, orient="index").rename(uppercase_text, axis="index")

# Processing scope
src_roots_df = prepare_dirs(src_roots)
src_roots_df = add_depth_metrics().execute(src_roots_df)
selected_roots_df = select_processing_targets(src_roots_df)
files_df, dirs_df = scan_directories(selected_roots_df)

# Pre-processing
files_df = add_file_path(prefix="").execute(files_df)
files_df = add_path_exists(prefix="").execute(files_df)
files_df = add_files_stat(prefix="").execute(files_df)
files_df = add_cache_key(exif_args, prefix="").execute(files_df)

new_files = files_df[~files_df["CacheKey"].isin(register_df.index)].set_index("CacheKey")
known_files = files_df[files_df["CacheKey"].isin(register_df.index)].set_index("CacheKey")

if not known_files.empty:
    date_change = register_df.loc[known_files.index, "ModifiedAt"] != known_files["ModifiedAt"] # risky check for float type
    size_change = register_df.loc[known_files.index, "Size"] != known_files["Size"]
    changed_files = known_files.loc[date_change | size_change]
    queue_df = pd.concat([new_files, changed_files])
else:
    queue_df = new_files

if not queue_df.empty:
    tqdm_desc = "Extracting exif metadata:"
    files_queue = queue_df["FilePath"].to_list()
    exif_results = [] # list of dfs
    for exif_result in tqdm(extract_exif_data(exif_path, files_queue, exif_args, batch_size=batch_size), total=len(files_queue), desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        exif_results.append(exif_result)
    exif_results_df = pd.DataFrame(exif_results)
    exif_results_df["SourceFile"] = exif_results_df["SourceFile"].apply(os.path.normpath)
    exif_results_df = exif_results_df.merge(files_df, how="left", left_on="SourceFile", right_on="FilePath")
    # Handle missed exif results
    unmatched = exif_results_df["CacheKey"].isna()
    if unmatched.any():
        print(f"{unmatched.sum()} exif results missed")
        exif_results_df = exif_results_df[~unmatched]
    exif_results_df = exif_results_df.set_index("CacheKey")
    # Update and save cache
    register_df = queue_df[["FilePath", "ModifiedAt", "Size"]].combine_first(register_df)
    register_df.to_json(registry_cache, orient="index", indent=4, force_ascii=False)
    data_df = exif_results_df.combine_first(data_df)
    data_dict = df_to_dict(data_df, drop_na=True)
    save_json(data_cache, data_dict)

ctx = Context(parser=DateParser(), geocoder=RGeocoder(mode=1, verbose=False))
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
metadata_df = assemble_dest_dir(ctx, dest_root, path_components).execute(metadata_df)

execution_df = files_df.merge(metadata_df[["DestFileDir"]], how="left", left_on="CacheKey", right_index=True)

if action == "move":
    execution_df = move_files(execution_df)
    dirs_df = remove_emptied_dirs(dirs_df)
elif action == "copy":
    execution_df = copy_files(execution_df)
else:
    raise RuntimeError("Unknown Action")

# Evaluate action results
completed = select_completed().execute(execution_df)
skipped = select_skipped().execute(execution_df)
failed = select_failed().execute(execution_df)

if not completed.empty:
    # Post-processing
    completed = add_file_path(prefix="Dest").execute(completed)
    completed = add_path_exists(prefix="").execute(completed) # re-check src
    completed = add_path_exists(prefix="Dest").execute(completed)
    completed = add_files_stat(prefix="Dest").execute(completed)
    completed = add_cache_key(exif_args, prefix="Dest").execute(completed)

    # update cache
    for cache_key, row in completed.iterrows():
        dest_cache_key = row["DestCacheKey"]
        dest_path = row["DestFilePath"]
        src_exists = row["PathExists"]

        if src_exists:
            register_df.loc[dest_cache_key] = register_df.loc[cache_key]
            data_df.loc[dest_cache_key] = data_df.loc[cache_key]
        else:
            register_df = register_df.rename(index={cache_key: dest_cache_key})
            data_df = data_df.rename(index={cache_key: dest_cache_key})
        
        register_df.loc[dest_cache_key, "FilePath"] = dest_path
        data_df.loc[dest_cache_key, "SourceFile"] = dest_path

    # save cache
    register_df.to_json(registry_cache, orient="index", indent=4, force_ascii=False)
    data_dict = df_to_dict(data_df, drop_na=True)
    save_json(data_cache, data_dict)

CSVWriter("output/completed.csv").save(completed)
CSVWriter("output/skipped.csv").save(skipped)
CSVWriter("output/failed.csv").save(failed)