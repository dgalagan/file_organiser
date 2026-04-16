from core.input_handling import get_user_input
from core.scanning import get_scope
from core.metadata import run_metadata_extraction, normalize_exif_datetime
from utils.path import get_file_extension
import pandas as pd
import sys
from datetime import datetime
import os
import shutil
from tqdm import tqdm

TARGET_ROOT = "D:\\MyOrganizedFiles"
# Reference
EXT_MAPPING = "ref\\ext_mapping.xlsx"
# Storage
STORAGE_CFG = {
    "db\\files_metadata.json": {
        "structure": {},
        "encoding": "utf-8",
        "indent": 4
    },
    "db\\hash_cfg.json": {
        "structure": {},
        "encoding": "utf-8",
        "indent": 4
    },
    "db\\exif_cfg.json": {
        "structure": {},
        "indent": 4
    },
}
EXIF_CFG = ["-j", "-all", "-G"]
HASH_CFG = {"hash_algo": "sha256", "parts": 8, "read_cap": 1024}


def main():
    # Obtain input from user
    try:
        input_dirs = get_user_input()
        dirs, files = get_scope(input_dirs)
    except Exception as e:
        print(e)
    # Extract metadata
    try:
        run_metadata_extraction(files, STORAGE_CFG, EXIF_CFG, HASH_CFG, reset_storage=False, batch_size=100)
    except Exception as e:
        print(e)
    # Assemble transition table
    files_md_df = pd.read_json("db\\files_metadata.json", orient="index")
    ref_df = pd.read_excel(EXT_MAPPING)
    transition_df = pd.DataFrame(index=files)
    transition_df.index.name = "FilePath"
    transition_df = (
        transition_df
        .join(files_md_df[["Hash", "File:FileName", "File:FileSize", "File:FileModifyDate","File:FileTypeExtension","EXIF:Model"]], how="left")
    )
    transition_df["isDuplicate"] = transition_df["Hash"].duplicated()
    transition_df["DuplicationStatus"] = transition_df["isDuplicate"].apply(lambda x: "unique" if not x else "duplicate")
    transition_df["File:FileModifyDate"] = transition_df["File:FileModifyDate"].apply(normalize_exif_datetime)
    transition_df["Year"] = transition_df["File:FileModifyDate"].apply(lambda x: datetime.fromtimestamp(x).year)
    transition_df["FileExtensionCalc"] = transition_df.index.to_series().apply(lambda x: get_file_extension(x).replace(".", "").lower())
    ref_df["FileExtension"] = ref_df["FileExtension"].str.lower()
    combined_df = pd.merge(transition_df.reset_index(), ref_df[["FileExtension", "Type"]], left_on="FileExtensionCalc", right_on="FileExtension", how="left")
    combined_df["TargetPath"] = combined_df.apply(
        lambda x: os.path.join(*[str(p) for p in [
            TARGET_ROOT,
            x["DuplicationStatus"],
            x["Type"],
            x["Year"],
            x["FileExtensionCalc"],
            x["EXIF:Model"],
            x["File:FileName"]
        ] if pd.notna(p)]), axis=1)
    # Check disk space
    files_size = combined_df["File:FileSize"].sum()
    _, _, free = shutil.disk_usage(TARGET_ROOT)
    if files_size >= free:
        print(f"Not enough space to move files: free {int(free /(1<<30))} GB, required {int(files_size /(1<<30))} GB")
        return 1
    # Copy and paste files
    desc = "Copying files into new structure"
    b_format = '{l_bar}{bar:60}{r_bar}{bar:-10b}'
    for index, row in tqdm(combined_df.iterrows(), total=len(combined_df), desc=f"{desc:<35}", bar_format=b_format):
        source = row["FilePath"]
        destination = row["TargetPath"]
        dest_dir = os.path.dirname(destination)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        shutil.copy2(source, destination)
    report_path = TARGET_ROOT + "\\" +"report.csv"
    combined_df.to_csv(report_path, encoding="utf-8-sig")
    return 0
    
    # try:
        # metadata_df = pd.DataFrame(files_metadata)
        # temp_df = metadata_df.replace('', np.nan)
        # coverage = temp_df.groupby('File:FileTypeExtension').apply(
        #     lambda group: group.notnull().mean() * 100
        # )
        # coverage.to_excel("coverage.xlsx")
        # metadata_df = metadata_df.map(lambda x: "icorrect char" if isinstance(x, str) and ILLEGAL_CHAR_RE.search(x) else x)
        # metadata_df.to_excel("metadata.xlsx")
        # return 0

if __name__ == "__main__":
    sys.exit(main())