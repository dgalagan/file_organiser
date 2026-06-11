from configs.cli_cfg import cli_objects, cli_grouped_objects
from configs.env_cfg import FILE_MANIFEST, FILE_PATHS, EXECUTABLE_MANIFEST, EXECUTABLE_PATHS, EXECUTABLE_URLS, ENCODING_CSV
from configs.db_cfg import DB_INIT_CFG, DB_RESET_FLAGS, DB_CALC
from configs.transformation_cfg import COLUMNS_ALIASES, PIPELINE
from cli.renderer import render_cli_object
from core.env_setup import download_tool
from core.dir_input import get_dest_dir, get_src_dirs
from core.processing_scope import collect_files_to_organise
from core.df_processor import DfProcessor, DfWriter
from core.transformation import fill_missing_values, assemble_dest_path
import os
import pandas as pd
import sys
import shutil
from tqdm import tqdm
from utils.text import uppercase_text
from utils.json import load_json, save_json
from utils.path import is_file

# [user input] instead of os.walk(), create recursion based on os.scandir()
# [user input] self-reporting improvement
# [user input] manage lowercase path cases in manual input
# [df] incorporate schema validation
# [df] automate file loading process
# [df] externalize ref and db merge
# [df] develop columns provision flow
# [df] divide dfprocessor class methods like transform, compute, filter into separate classes
# [dest path] .compute(function needed, store_col="Location", col_names=["EXIF:GPSLatitude", "EXIF:GPSLongitude"])
# [dest path] use id3:year for music

path_components = ["DuplicateLabel", "Category", "Year", "CameraModel", "FileExtension", "CountWorksheets", "FileName"]
report_cols = ["FileName", "FileSize", "FileExtension", "Category", "DuplicateLabel", "Year", "CameraModel", "CountWorksheets", "DestPath"]
report_name = "migration_report"

def main():
    
    #########        SETUP ENV       #########

    # Resolve DB dependency
    db_files = FILE_MANIFEST.get("db", [])
    for db_file in db_files:
        db_path = FILE_PATHS.get(db_file, '')
        if not db_path:
            raise RuntimeError(f"No path resolved for {db_file}")
        reset = DB_RESET_FLAGS.get(db_file, False)
        if not is_file(db_path) or reset:
            try:
                container = DB_INIT_CFG.get(db_file, {})
                save_json(db_path, container)
            except (TypeError, IOError) as e:
                raise RuntimeError(f"Failed to init {db_path}") from e
        print(f"✅ {db_file} available")
    
    # Resolve tools dependency
    tools = EXECUTABLE_MANIFEST.get("bin", [])
    for tool in tools:
        executable_path = EXECUTABLE_PATHS.get(tool, '')
        if not executable_path:
            raise RuntimeError(f"No path resolved for {tool}")
        if not is_file(executable_path):
            url = EXECUTABLE_URLS[tool]
            tool_path = os.path.dirname(executable_path)
            if not url:
                raise RuntimeError(f"{tool} not found and no download URL configured")
            try:
                download_tool(url, tool_path)
            except:
                raise RuntimeError(f"Failed to download {tool}") from e
        print(f"✅ {tool} available")
    
    #########       USER INPUT       #########
    try:
        dest_dir = get_dest_dir(cli_objects)
        if not dest_dir:
            return 1
        src_dirs = get_src_dirs(cli_grouped_objects, cli_objects)
        if not src_dirs:
            return 1
    except Exception as e:
        print(e)

    #########    PROCESSING SCOPE    #########
    
    try:
        dirs, files_to_organise = collect_files_to_organise(src_dirs, cli_objects=cli_objects)
    except Exception as e:
        print(e)

    #########      EXTRACT DATA      #########
    
    # Initialize runtime data containers
    processing_queue = {db_file: [] for db_file in db_files}
    runtime_data = {db_file: {} for db_file in db_files}
    runtime_size = {}
    runtime_mtime = {}

    # Prepare files processing queues
    tqdm_desc = "Prepare files processing queue:"
    for db_file in db_files:
        try:
            db_data = load_json(FILE_PATHS[db_file])
        except (FileNotFoundError, IOError, PermissionError) as e:
            raise RuntimeError(f"Failed to load {db_file}") from e
        for file in tqdm(files_to_organise, desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
            # Get current mtime and size
            runtime_size[file] = os.path.getsize(file)
            runtime_mtime[file] = os.path.getmtime(file)
            # Get cached data
            file_history = db_data.get(file, {})
            cached_snapshot = file_history.get(DB_CALC[db_file]["cfg_str"], {})
            cached_data = cached_snapshot.get("data")
            cached_mtime = cached_snapshot.get("mtime")
            cached_size = cached_snapshot.get("size")
            # Add file to processing queue or reuse cached data for this runtime
            if not file_history or not cached_snapshot or runtime_mtime[file] != cached_mtime or runtime_size[file] != cached_size:
                processing_queue[db_file].append(file)
            else:
                runtime_data[db_file][file] = cached_data
        # Log the total number of files that require processing
        print(f"{len(processing_queue[db_file])} file(s) queued for [{db_file}] processing")

    print(render_cli_object(cli_objects["divider"]))
    # Handle processing queue
    for db_file, files_to_process in processing_queue.items():
        # Load storage
        try:
            db_data = load_json(FILE_PATHS[db_file])
        except (FileNotFoundError, IOError, PermissionError) as e:
            raise RuntimeError(f"Failed to load {db_file}") from e
        # Load storage config
        extraction_cfg = DB_CALC.get(db_file, {})
        cfg_str = extraction_cfg.get("cfg_str", '')
        cfg = extraction_cfg.get("cfg", {})
        func = extraction_cfg.get("func")
        if cfg_str and cfg and func is not None:
            for processed_file, data in func(files_to_process, cfg):
                # Update runtime container
                runtime_data[db_file][processed_file] = data
                # Update storage
                if processed_file not in db_data:
                    db_data[processed_file] = {cfg_str: {"data":data, "mtime":runtime_mtime[processed_file], "size":runtime_size[processed_file]}}
                else:
                    db_data[processed_file][cfg_str] = {"data":data, "mtime":runtime_mtime[processed_file], "size":runtime_size[processed_file]}
        # Save updated data
        try:
            save_json(FILE_PATHS[db_file], db_data)
        except (TypeError, IOError, PermissionError) as e:
            raise RuntimeError(f"Failed to save {db_file}") from e
    
    print(render_cli_object(cli_objects["divider"]))
    #########     TRANSFORM DATA     #########
    
    # Load ref data into df
    refdata_dfs = {}
    ref_files = FILE_MANIFEST.get("ref", [])
    for ref_file in ref_files:
        try:
            refdata = load_json(FILE_PATHS[ref_file])
            refdata_df = pd.DataFrame.from_dict(refdata, orient="index")
            refdata_df = refdata_df.rename(columns=COLUMNS_ALIASES[ref_file]).rename(uppercase_text, axis="index")
            refdata_dfs[ref_file] = refdata_df
        except Exception as e:
            print(e)

    # Load runtime data into df
    metadata_dfs = {}
    for runtime_key, runtime_dict in runtime_data.items():
        metadata_df = pd.DataFrame.from_dict(runtime_dict, orient="index")
        metadata_df = metadata_df.rename(columns=COLUMNS_ALIASES[runtime_key])
        metadata_df = DfProcessor(metadata_df).run_pipeline(PIPELINE[runtime_key]).df
        metadata_dfs[runtime_key] = metadata_df
    full_metadata = pd.concat(metadata_dfs.values(), axis=1)

    # Join refdata
    refdata_df = refdata_dfs["extension_ref.json"]
    enriched_df = pd.merge(full_metadata, refdata_df[["Category"]], how="left", left_on="FileExtension", right_index=True)
    df_processor = DfProcessor(enriched_df)
    (
        df_processor
        .transform(fill_missing_values("Other"), func_mode="col", use_cols="Category")
        .compute(assemble_dest_path(dest_dir), func_mode="row", calc_col="DestPath", use_cols=path_components)
    )
    report_df = df_processor.filter_cols(names=report_cols).active_selection
    report_path = os.path.join(dest_dir, "migration_report.csv")
    DfWriter.write(report_df, extension="csv", filepath=report_path, encoding=ENCODING_CSV)
    print(render_cli_object(cli_objects["divider"]))
    #########     CHECK DISK SPACE    #########
    
    files_size = report_df["FileSize"].sum()
    _, _, free = shutil.disk_usage(dest_dir)
    if files_size >= free:
        print(f"Not enough space to move files: free {int(free /(1<<30))} GB, required {int(files_size /(1<<30))} GB")
        return 1
    
    ########        MOVE FILES        #########
    
    # desc = "Copying files into new structure"
    # b_format = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
    # for index, row in tqdm(combined_df.iterrows(), total=len(combined_df), desc=f"{desc:<35}", bar_format=b_format):
    #     source = row["FilePath"]
    #     destination = row["TargetPath"]
    #     dest_dir = os.path.dirname(destination)
    #     if not os.path.exists(dest_dir):
    #         os.makedirs(dest_dir) 
    #     shutil.copy2(source, destination)
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    if exit_code == 1:
        print(render_cli_object(cli_objects["flow_marker"]))
        print(render_cli_object(cli_objects["info"], "exit"))
    sys.exit(exit_code)