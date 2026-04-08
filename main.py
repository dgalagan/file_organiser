from core.input_handling import get_user_input
from core.scanning import get_scope
from core.metadata import get_basic_metadata, prepare_processing_queue, extract_hash, extract_metadata
import pandas as pd
import sys
import traceback
BASIC_METADATA = "db\\basic_metadata.xlsx"
DETAILED_METADATA = "db\\detailed_metadata.xlsx"
HASH = "db\\hash.xlsx"

RUN_SCANNER = False

def main():
    try:
        input_dirs = get_user_input()
        dirs, files = get_scope(input_dirs)
        basic_metadata = get_basic_metadata(files, db_link=BASIC_METADATA)
        processing_queue = prepare_processing_queue(db_link=BASIC_METADATA)
        hash = extract_hash(processing_queue, db_link=HASH)
        detailed_metadata = extract_metadata(processing_queue, db_link=DETAILED_METADATA)
        print(detailed_metadata)
        # processing_queue = prepare_processing_queue()
        # files_metadata = extract_metadata(files)
        # metadata_df = pd.DataFrame(files_metadata)
        # temp_df = metadata_df.replace('', np.nan)
        # coverage = temp_df.groupby('File:FileTypeExtension').apply(
        #     lambda group: group.notnull().mean() * 100
        # )
        # coverage.to_excel("coverage.xlsx")
        # metadata_df = metadata_df.map(lambda x: "icorrect char" if isinstance(x, str) and ILLEGAL_CHAR_RE.search(x) else x)
        # metadata_df.to_excel("metadata.xlsx")
        return 0
    except Exception:
        print("--- CRASH REPORT ---")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    if RUN_SCANNER:
        sys.exit(main())
    else:
        print("do smth else")
        b = pd.read_excel(BASIC_METADATA).set_index("FilePath")
        h = pd.read_excel(DETAILED_METADATA).set_index("FilePath")
        e = pd.read_excel(HASH).set_index("FilePath")
        date_cols = [col for col in h.columns if 'date' in col.lower()]
        for col in date_cols:
            h[col] = pd.to_datetime(h[col], errors='coerce', utc=True, format='%Y:%m:%d %H:%M:%S%z')
            h[col] = h[col].dt.tz_localize(None)
        h[date_cols].to_csv("date.csv")