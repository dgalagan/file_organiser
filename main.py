import pandas as pd
from enum import StrEnum, auto
from pipelines import prepare_dirs, add_depth_metrics, add_file_path, add_path_exists, add_files_stat, add_cache_key, select_completed, select_failed, select_skipped, tag_columns, select_columns, resolve_extension, select_extension, normalize_categories, label_duplicates, extract_year, extract_country, extract_worksheets_count, assemble_dest_dir
from cli.tokens import Icon, Separator
from cli.components import Info, Prompt
from core.transformation import DateParser
from config import Config, Cache, Exif, Reference
from dataframe.context import Context
from dataframe.write import CSVWriter, JSONWriter
from dataframe.load import JSONLoader
from dotenv import load_dotenv
import os
import pandas as pd
from reverse_geocoder import RGeocoder
import shutil
from tqdm import tqdm
from typing import Literal
from utils.path import iter_dir_hierarchy, is_parent, depth_from_dir
from utils.text import uppercase_text

load_dotenv()

#########        TO DO LIST      #########
# [user input] instead of os.walk(), create recursion based on os.scandir()
# [user input] self-reporting improvement
# [user input] manage lowercase path cases in manual input
# [user input] update menu for depth input provision(enter=skip)
# [df] externalize ref and db merge
# [df] Rename Predicate class into RowMask or RowFilter, remove where from Compute and Transform
# [df] develop partial hash function
# i need to be able to move things back
# i need a session id

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

def rollback(files_df: pd.DataFrame) -> pd.DataFrame:
    return files_df

# MAIN FUNCTION
def organise_files(src_roots: list[str], dest_root: str, dest_structure: list[str], operation: Literal["copy", "move"], config: Config) -> None:

    if operation not in ("copy", "move"):
        raise ValueError(f"Unknown operation: {operation}")

    # map 
    dest_structure = [STRUCTURE_ALIASES[name] for name in dest_structure if name in STRUCTURE_ALIASES]

    # Load cache tables
    register_df = config.register.load()
    data_df = config.data.load()
    
    # Load ref table
    ref_df = config.ref.load().rename(uppercase_text, axis="index")

    # Processing scope
    src_roots_df = pd.DataFrame({"DirPath": src_roots, "IsInvalid": False, "IsDuplicate": False,})
    src_roots_df = prepare_dirs(src_roots_df)
    src_roots_df = add_depth_metrics().execute(src_roots_df)
    selected_roots_df = select_processing_targets(src_roots_df)
    files_df, dirs_df = scan_directories(selected_roots_df)

    # Pre-processing
    files_df = add_file_path(prefix="").execute(files_df)
    files_df = add_path_exists(prefix="").execute(files_df)
    files_df = add_files_stat(prefix="").execute(files_df)
    files_df = add_cache_key(config.exif.args, prefix="").execute(files_df)

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
        for exif_result in tqdm(config.exif.extract(files_queue), total=len(files_queue), desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
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
        # Update cache tables
        register_df = queue_df[["FilePath", "ModifiedAt", "Size"]].combine_first(register_df)
        data_df = exif_results_df.combine_first(data_df)

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
    metadata_df = assemble_dest_dir(ctx, dest_root, dest_structure).execute(metadata_df)

    # Transfer files into new structure
    execution_df = files_df.merge(metadata_df[["DestFileDir"]], how="left", left_on="CacheKey", right_index=True)
    desc = f"{operation} files into new structure"
    for row_id, row in tqdm(execution_df.iterrows(), total=len(execution_df), desc=f"{desc:<40}", bar_format='{l_bar}{bar:60}{r_bar}{bar:-10b}'):

        src_path = os.path.join(row["FileDir"], row["FileName"])
        dest_path = os.path.join(row["DestFileDir"], row["FileName"])

        if src_path == dest_path:
            execution_df.at[row_id, "Status"] = "SKIPPED"
            execution_df.at[row_id, "ErrorMessage"] = "no action required"
            continue

        if os.path.exists(dest_path):
            execution_df.at[row_id, "Status"] = "SKIPPED"
            execution_df.at[row_id, "ErrorMessage"] = "duplicate"
            continue

        try:
            os.makedirs(row["DestFileDir"], exist_ok=True)
            if operation == "copy":
                shutil.copy2(src_path, dest_path)
            elif operation == "move":
                shutil.move(src_path, dest_path)
                if os.path.exists(src_path):
                    raise RuntimeError("Source still exists")
            execution_df.at[row_id, "Status"] = "COMPLETED"
        except Exception as e:
            execution_df.at[row_id, "Status"] = "FAILED"
            execution_df.at[row_id, "ErrorMessage"] = str(e)

    if operation == "move":
        dirs_df = remove_emptied_dirs(dirs_df)

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
        completed = add_cache_key(config.exif.args, prefix="Dest").execute(completed)

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

    # save cache tables
    config.register.save(register_df)
    config.data.save(data_df)

    csv_writer = CSVWriter(encoding="utf-8-sig")
    csv_writer.save(completed, "output\\completed.csv")
    csv_writer.save(skipped, "output\\skipped.csv")
    csv_writer.save(failed, "output\\failed.csv")

if __name__ == "__main__":
    
    exif_path = find_exiftool()
    clear_cache = False

    config = Config(
        register=Cache(
            path="cache/register.json",
            clear_cache=clear_cache,
            writer=JSONWriter(orient="index", indent=4, force_ascii=False, drop_na=False),
            loader=JSONLoader(orient="index")
        ),
        data=Cache(
            path="cache/data.json",
            clear_cache=clear_cache,
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
        )
    )

    organise_files(
        src_roots=["D:\\MyOrganizedFiles"],
        dest_root="D:\\MyOrganizedFiles",
        dest_structure=["DuplicateLabel", "FileCategory", "CreationYear", "FileExtension", "CameraModel", "ImageCountry", "WorksheetCount"],
        operation="move",
        config=config
    )