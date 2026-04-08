from exiftool import ExifToolHelper
import hashlib
import pandas as pd
from utils.path import is_file, is_readonly, is_hidden, is_system, get_file_extension
from tqdm import tqdm
import os
import re

ILLEGAL_CHAR_RE = re.compile(r'[\000-\010]|[\013-\014]|[\016-\037]|[\ufffe-\uffff]')

def get_basic_metadata(files: list[str], db_link=None):
    basic_metadata = pd.read_excel(db_link).set_index("FilePath", inplace=True)
    existing_files = set(basic_metadata.index)
    input_files = set(files)
    files_to_add = input_files - existing_files
    files_to_update = input_files & existing_files
    files_out_of_scope = existing_files - input_files
    new_entries = []
    for file_to_add in tqdm(files_to_add, desc="Add new entries"):
        try:
            stat = os.stat(file_to_add)
            new_entries.append({
                "FilePath": file_to_add,
                "FileExt": get_file_extension(file_to_add),
                "isFile": is_file(file_to_add),
                "isReadonly": is_readonly(file_to_add),
                "isHidden": is_hidden(file_to_add),
                "isSystem": is_system(file_to_add),
                "UserId": stat.st_uid,
                "GroupId": stat.st_gid,
                "Size": stat.st_size,
                "LastAccessDate": stat.st_atime,
                "LastModifiedDate": stat.st_mtime,
                "CreatedDate": stat.st_birthtime,
                "ProcessingStatus": "NEW"
            })
        except FileNotFoundError:
            continue
    if new_entries:
        new_data = pd.DataFrame(new_entries).set_index("FilePath")
        basic_metadata = pd.concat([basic_metadata, new_data])
    updated_entries = []
    for file_to_update in tqdm(files_to_update, desc="Update existing entries"):
        try:
            stored_mtime = basic_metadata.at[file_to_update, "LastModifiedDate"]
            stored_size = basic_metadata.at[file_to_update, "Size"]
            stat = os.stat(file_to_update)
            if (stat.st_mtime - stored_mtime) > 0.00001 or stat.st_size != stored_size:
                updated_entries.append({
                    "FilePath": file_to_update,
                    "FileExt": get_file_extension(file_to_update),
                    "isFile": is_file(file_to_update),
                    "isReadonly": is_readonly(file_to_update),
                    "isHidden": is_hidden(file_to_update),
                    "isSystem": is_system(file_to_update),
                    "UserId": stat.st_uid,
                    "GroupId": stat.st_gid,
                    "Size": stat.st_size,
                    "LastAccessDate": stat.st_atime,
                    "LastModifiedDate": stat.st_mtime,
                    "CreatedDate": stat.st_birthtime,
                    "ProcessingStatus": "UPDATED"
                })
            else:
                updated_entries.append({
                    "FilePath": file_to_update,
                    "ProcessingStatus": "CURRENT"
                })
        except (FileNotFoundError, KeyError):
            continue
    if updated_entries:
        updated_data = pd.DataFrame(updated_entries).set_index("FilePath")
        basic_metadata.update(updated_data)
    out_of_scope_entries = []
    for file in files_out_of_scope:
        out_of_scope_entries.append({
            "FilePath": file,
            "ProcessingStatus": "EXCLUDED"
        })
    if out_of_scope_entries:
        out_of_scope_data = pd.DataFrame(out_of_scope_entries).set_index("FilePath")
        basic_metadata.update(out_of_scope_data)

    basic_metadata.reset_index().to_excel(db_link, index=False)
    
    return basic_metadata

def prepare_processing_queue(db_link=None):
    basic_metadata = pd.read_excel(db_link).set_index("FilePath", inplace=True)
    processing_queue = basic_metadata[basic_metadata["ProcessingStatus"].isin(["NEW", "UPDATED"])]
    return processing_queue.sort_values("Size").index.tolist()

def get_file_hash(path, parts=8, hash_algo="sha256", read_bytes=1024):
    hash_func = getattr(hashlib, hash_algo)
    file_size = os.path.getsize(path)
    part_size = file_size // parts
    remainder = file_size % parts
    byte_steps = [part_size * step for step in range(parts)]
    combined_hash = hash_func()
    try:
        with open(path, "rb") as f:
            for byte_step in byte_steps:
                f.seek(byte_step, 0)
                data = f.read(read_bytes)
                combined_hash.update(data)
        return combined_hash.hexdigest()
    except PermissionError:
        return None

def extract_hash(files: list[str], db_link=None):
    hash_data = pd.read_excel(db_link).set_index("FilePath", inplace=True)
    new_hashes = []
    updated_hashes = []
    for file in tqdm(files, desc="Calculating hash"):
        if file not in hash_data.index:
            new_hashes.append({
                "FilePath": file,
                "Hash": get_file_hash(file)
            })
        else:
            updated_hashes.append({
                "FilePath": file,
                "Hash": get_file_hash(file)
            })
    if new_hashes:
        new_hashes_data = pd.DataFrame(new_hashes).set_index("FilePath")
        hash_data = pd.concat([hash_data, new_hashes_data])
    if updated_hashes:
        updated_hashes_data = pd.DataFrame(updated_hashes).set_index("FilePath")
        hash_data.update(updated_hashes_data)
    hash_data.reset_index().to_excel(db_link, index=False)
    return hash_data

def extract_metadata(files: list[str], batch_size=100, db_link=None) -> list[dict]:
    detailed_metadata = pd.read_excel(db_link).set_index("FilePath", inplace=True)
    existing_files = detailed_metadata.index
    all_metadata = []
    try:
        with ExifToolHelper(encoding="utf-8") as et:
            for i in tqdm(range(0, len(files), batch_size)):
                chunk = files[i : i + batch_size]
                try:
                    # Try to process the whole chunk at high speed
                    batch_results = et.get_metadata(chunk)
                    all_metadata.extend(batch_results)
                except Exception as e:
                    for file_path in chunk:
                        try:
                            file_metadata = et.get_metadata(file_path)
                            all_metadata.extend(file_metadata)
                        except Exception as e:
                            all_metadata.append({"SourceFile": file_path})
    except:
        print(e)
    if all_metadata:
        results_df = pd.DataFrame(all_metadata).rename(columns={"SourceFile": "FilePath"}).set_index("FilePath", inplace=True)
        is_update = results_df.index.isin(existing_files)
        df_to_update = results_df[is_update]
        df_to_add = results_df[~is_update]
        if not df_to_update.empty:
            detailed_metadata.update(df_to_update)
        if not df_to_add.empty:
            detailed_metadata = pd.concat([detailed_metadata, df_to_add])
        detailed_metadata = detailed_metadata.map(lambda x: "icorrect char" if isinstance(x, str) and ILLEGAL_CHAR_RE.search(x) else x)
        detailed_metadata.reset_index().to_excel(db_link, index=False)
    return detailed_metadata