from cli.tokens import Emoji, Icon, Separator
from cli.components import Info
from core.exiftool import find_exiftool, extract_exif_data
from core.dir_input import get_dest_dir, get_input_data
from core.transformation import DateParser, calc_full_hash
from dataframe.save import CSVWriter
from dotenv import load_dotenv
import os
import pandas as pd
from pipelines import exif_pipeline, build_path_pipeline
from reverse_geocoder import RGeocoder
import sys
import shutil
from tqdm import tqdm
from utils.text import uppercase_text
from utils.json import load_json, save_json
from utils.path import iter_dir_hierarchy, is_file, get_dir_depth
import warnings
import hashlib
import json

load_dotenv()

#########        TO DO LIST      #########
# [user input] instead of os.walk(), create recursion based on os.scandir()
# [user input] self-reporting improvement
# [user input] manage lowercase path cases in manual input
# [user input] update menu for depth input provision(enter=skip)
# [db] store file metadata in separate .json files with hash_key as a name (hash key = hashed(exif args + filename))
# [df] externalize ref and db merge
# [df] Rename Predicate class into RowMask or RowFilter, remove where from Compute and Transform
# [df] develop partial hash function

COLUMN_ALIASES = {
    "exif": {
        "File:FileName"             : "FileName",
        "File:FileSize"             : "FileSize",
        "File:FileTypeExtension"    : "ExifExtension",
        "XML:HeadingPairs"          : "DocumentStructure",
        "EXIF:Model"                : "CameraModel",
        "EXIF:GPSLatitude"          : "Latitude",
        "EXIF:GPSLongitude"         : "Longitude",
    },
    "ref": {
        "magic_number"              : "MagicNumber",
        "software"                  : "Software",
        "description"               : "Desc",
        "category"                  : "Category",
    }
}
path_components = ["DuplicateLabel", "Category", "Year", "CameraModel", "Country", "ExifExtension", "CountWorksheets"]
report_cols = ["FileName", "FileSize", "ExifExtension", "Category", "DuplicateLabel", "Year", "CameraModel", "Country", "CountWorksheets", "DestDir"]

def main():
    
    #########        SETUP ENV       #########
    
    exif_path = find_exiftool()
    
    #########       USER INPUT       #########
    try:
        dest_dir = get_dest_dir()
        if not dest_dir:
            return 1
        input_data = get_input_data()
        if input_data.empty:
            return 1
    except Exception as e:
        print(e)

    #########    PROCESSING SCOPE    #########
    
    try:
        tqdm_desc = "Extracting files from input dirs:"
        dirs, files = set(), set()
        report = {}
        for row_id, row in input_data.iterrows():
            input_dir = row["DirPath"]
            input_depth = row["UserInputDepth"]
            if input_dir not in report:
                report[input_dir] = {}
            print("\n".join([Separator.DASH.repeat(100), Info.ELEMENTS["extracting"].generate(path=input_dir), Icon.DOWNARROW.repeat(3)]))
            for depth, dir, filenames in iter_dir_hierarchy(input_dir, input_depth):
                dirs.add(dir)
                if depth not in report[input_dir]:
                    report[input_dir][depth] = {}
                for filename in filenames:
                    file_path = os.path.join(dir, filename)
                    files.add(file_path)
                # if filenames:
                report[input_dir][depth][dir] = len(filenames)
            # print report
            files_per_lvl = 0
            dirs_per_lvl = 0
            for lvl, dirs_scanned in report[input_dir].items():
                dirs_per_lvl += len(dirs_scanned)
                for dir_scanned, files_extracted in dirs_scanned.items():
                    files_per_lvl += files_extracted
                    # print(f"lvl {lvl:<2} | {os.path.basename(dir_scanned):<30.30} ---> {files_extracted:<4} files extracted")
                print(f"lvl {lvl:<2} | {dirs_per_lvl:<4} dirs scanned | {files_per_lvl:<4} files extracted")
                files_per_lvl = 0
                dirs_per_lvl = 0
        print(Separator.DASH.repeat(100))
        print(" ".join([Emoji.BULLSEYE, f"[Total] {len(dirs)} dirs scanned | {len(files)} files extracted"]))
        print(Separator.DASH.repeat(100))
    except Exception as e:
        print(f"File Extraction failed {e}")

    #########    EXTRACT EXIF DATA   #########
    reset = False
    if reset:
        register = save_json("db/register.json", {})
        data = save_json("db/data.json", {})

    register = load_json("db/register.json")
    data = load_json("db/data.json")

    exif_params = ["-j", "-G"]
    include_tags = ["-all"]
    exclude_tags = ["--File:Directory"]
    exif_args = exif_params + include_tags + exclude_tags
    batch_size = 50

    ingest_queue = {}
    lookup_queue = {}
    runtime_data = {}

    tqdm_desc = "Cheking exif register:"
    for file in tqdm(files, total=len(files), desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        mtime = os.path.getmtime(file)
        size = os.path.getsize(file)
        st_dev = os.stat(file).st_dev
        st_ino = os.stat(file).st_ino
        hash_key = hashlib.md5(f"{st_ino}{json.dumps(sorted(exif_args))}".encode()).hexdigest()
        lookup_queue[file] = hash_key # temporary storage
        stored_data = register.get(hash_key, {})
        stored_mtime = stored_data.get("mtime")
        stored_size = stored_data.get("size")
        if not stored_data or stored_mtime != mtime or stored_size != size:
            register[hash_key] = {"mtime": mtime, "size": size, "file": file}
            ingest_queue[file] = hash_key # temporary storage

    tqdm_desc = "Extracting exif metadata:"
    for exif_result in tqdm(extract_exif_data(exif_path, list(ingest_queue.keys()), exif_args, batch_size=batch_size), total=len(ingest_queue), desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"):
        file = exif_result.get("SourceFile", "").replace('/', os.sep)
        hash_key = ingest_queue[file] # temporary storage
        data[hash_key] = exif_result
        ingest_queue.pop(file, None)# temporary storage

    if ingest_queue:
        for file in ingest_queue:
            warnings.warn(f"[{file}] has no metadata extracted")

    # Pull data into runtime
    tqdm_desc = "Loading exif metadata:"
    for file, hash_key in tqdm(lookup_queue.items(), desc=f"{tqdm_desc:<40}", bar_format="{l_bar}{bar:60}{r_bar}{bar:-10b}"): # temporary storage
        runtime_data[file] = data.get(hash_key, {}) # temporary storage
    
    #########     TRANSFORM DATA     #########
    
    # Load ref data into df
    refdata = load_json("ref/extension_ref.json")
    refdata_df = pd.DataFrame.from_dict(refdata, orient="index")
    refdata_df = refdata_df.rename(columns=COLUMN_ALIASES["ref"]).rename(uppercase_text, axis="index")

    # Load runtime data into df
    metadata_df = pd.DataFrame.from_dict(runtime_data, orient="index")
    date_parser = DateParser()
    geocoder = RGeocoder(mode=1, verbose=False)
    metadata_df = exif_pipeline(date_parser, geocoder).execute(metadata_df)
    metadata_df = metadata_df.rename(columns=COLUMN_ALIASES["exif"])
    CSVWriter("output", "metadata_report").save(metadata_df)
    
    # Join refdata
    enriched_df = pd.merge(metadata_df, refdata_df[["Category"]], how="left", left_on="ExifExtension", right_index=True)
    enriched_df = build_path_pipeline(dest_dir, path_components).execute(enriched_df)
    
    # Save report
    report_df = enriched_df[report_cols]
    CSVWriter("output", "migration_report").save(report_df)
        
    ########        COPY/MOVE FILES        #########
    execution_df = pd.DataFrame(files, columns=["SrcPath"], index=list(files))
    execution_df["DestDir"] = report_df["DestDir"]
    execution_df["FileName"] = report_df["FileName"]
    execution_df["Status"] = None
    while True:
        print('\n'.join(["Type 'copy' - source files unchanged", "Type 'move' - source files removed"]))
        mode = input("Provide your option: ")
        if mode == "copy":
            # Check available space on the drive
            files_size = report_df["FileSize"].sum()
            _, _, free = shutil.disk_usage(dest_dir)
            if files_size >= free:
                print(f"Not enough space to copy files: free {int(free /(1<<30))} GB, required {int(files_size /(1<<30))} GB")
                return 1
            # Copy Files
            desc = "Copying files into new structure"
            for row_id, row in tqdm(execution_df.iterrows(), total=len(execution_df), desc=f"{desc:<40}", bar_format='{l_bar}{bar:60}{r_bar}{bar:-10b}'):
                source_path = row["SrcPath"]
                dest_dir = row["DestDir"]
                filename = row["FileName"]
                # copy files
                try:
                    os.makedirs(dest_dir, exist_ok=True)
                    dest_path = os.path.join(dest_dir, filename)
                    shutil.copy2(source_path, dest_path)
                    execution_df.at[row_id, "Status"] = f"COPIED"
                except Exception as e:
                    execution_df.at[row_id, "Status"] = f"ERROR - {e}"
            
            break
        elif mode == "move":
            desc = "Move files into new structure"
            for row_id, row in tqdm(execution_df.iterrows(), total=len(execution_df), desc=f"{desc:<40}", bar_format='{l_bar}{bar:60}{r_bar}{bar:-10b}'):
                source_path = row["SrcPath"]
                dest_dir = row["DestDir"]
                filename = row["FileName"] 
                # move files
                try:
                    os.makedirs(dest_dir, exist_ok=True)
                    dest_path = os.path.join(dest_dir, filename)
                    
                    # getdata before move
                    source_st_ino = os.stat(source_path).st_ino
                    
                    shutil.move(source_path, dest_path)
                    
                    # getdata after move
                    dest_st_ino = os.stat(dest_path).st_ino
                    
                    hash_id = lookup_queue.get(source_path, "")
                    if source_st_ino == dest_st_ino:
                        register[hash_id]["file"] = dest_path
                        data[hash_id]["SourceFile"] = dest_path
                    else:
                        new_hash_id = hashlib.md5(f"{dest_st_ino}{json.dumps(sorted(exif_args))}".encode()).hexdigest()
                        register[new_hash_id] = register.pop(hash_id) | {"file": dest_path}
                        data[new_hash_id] = data.pop(hash_id) | {"SourceFile": dest_path}
                    
                    # Update status
                    execution_df.at[row_id, "Status"] = f"MOVED"
                except Exception as e:
                    execution_df.at[row_id, "Status"] = f"ERROR - {e}"
            # cleanup src dirs
            dir_hierarchy = {}
            for dir in dirs:
                dir_depth = get_dir_depth(dir)
                if not dir_depth in dir_hierarchy:
                    dir_hierarchy[dir_depth] = [dir]
                else:
                    dir_hierarchy[dir_depth].append(dir)
            deepest_to_highest = sorted(dir_hierarchy, reverse=True)
            for hierarchy_lvl in deepest_to_highest:
                for dir in dir_hierarchy[hierarchy_lvl]:
                    content = os.listdir(dir)
                    if content:
                        print(f"{content} left in {dir}, could not be deleted")
                    else:
                        os.rmdir(dir)
            break
        else:
            print("invalid_input")
            continue


    save_json("db/data.json", data)
    save_json("db/register.json", register)
    print(Separator.DASH.repeat(100))

    CSVWriter("output", "execution_report").save(execution_df)

    return 0

if __name__ == "__main__":
    exit_code = main()
    if exit_code == 1:
        print(Icon.DOWNARROW.repeat(3))
        print(("exit"))
    sys.exit(exit_code)