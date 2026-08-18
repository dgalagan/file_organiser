from core.transformation import calc_full_hash
from dataframe.pipeline import Pipeline, AssignTags, FilterCols, FilterRows, Compute
from dataframe.col_filter import NameFilter, KeywordFilter, TagFilter, CombinedFilter
from dataframe.processor import ElementProcessor, RowProcessor, ColProcessor
from dataframe.predicate import Predicate, Condition, And, Or, AllRows
from dataframe.context import Context
import datetime as dt
import pandas as pd
from reverse_geocoder import RGeocoder
from utils.path import is_not_dir, get_normalized_path, depth_from_drive, tree_depth, parse_filename
from utils.text import lowercase_text, uppercase_text
import os
import hashlib
from constants import Cols, Tags
from typing import Literal

###############################
########### HELPERS ###########
###############################

def dest_col(base: str):
    return f"Dest{base}"

def dup_col(base: str) -> str:
    return f"{base}Dup"

def dup_label_col(base: str) -> str:
    return f"{base}DupLabel"

def duplicated_ci(df: pd.DataFrame, keep: Literal[False, "first", "last"]) -> pd.DataFrame:
    return df.iloc[:, 0].str.lower().duplicated(keep=keep)

def duplicated(df: pd.DataFrame, keep: Literal[False, "first", "last"]) -> pd.DataFrame:
    return df.duplicated(keep=keep)

def label_bool(value: bool, labels: dict) -> str:
    if pd.isna(value):
        return None
    return labels.get(value, None)

def safe_stat(file_path: str) -> os.stat_result | None:
    if os.path.isfile(file_path):
        try:
            return os.stat(file_path)
        except OSError:
            return None
def get_size(stat: os.stat_result) -> int | None:
    return stat.st_size if stat is not None else None
def get_mtime(stat: os.stat_result) -> int | None:
    return stat.st_mtime if stat is not None else None
def get_ino(stat: os.stat_result) -> int | None:
    return stat.st_ino if stat is not None else None
def get_dev(stat: os.stat_result) -> int | None:
    return stat.st_dev if stat is not None else None
def get_id(stat: os.stat_result) -> str | None:
    return hashlib.md5(f"{stat.st_dev}|{stat.st_ino}".encode()).hexdigest() if stat is not None else None

def build_unique_filename(filename: str, ino: int) -> str:
    stem, ext = parse_filename(filename)
    return f"{stem}_{str(ino)}.{ext}"

def build_file_path(row: pd.Series) -> str:
    dir_path = next(row[col] for col in row.index if Cols.FILE_DIR_PATH in col)
    filename = build_unique_filename(row[Cols.FILE_NAME], row[Cols.INODE]) if row.get(dup_col(Cols.FILE_NAME), False) else row[Cols.FILE_NAME]
    return os.path.join(dir_path, filename)

def build_dir_path(row: pd.Series, root: str) -> str:
    components = [str(value) for value in row if pd.notna(value)]
    return os.path.join(root, *components)

def resolve_ext(row: pd.Series) -> str:
    _, ext = parse_filename(row[Cols.FILE_NAME])
    exif_ext = row[Cols.FILE_TYPE_EXT]
    return uppercase_text(ext) if pd.isna(exif_ext) else exif_ext

def get_country(row: pd.Series, geocoder: RGeocoder) -> str:
    lat = row[Cols.EXIF_GPS_LATITUDE]
    lon = row[Cols.EXIF_GPS_LONGITUDE]
    
    if pd.isna(lat) or pd.isna(lon):
        return None

    return geocoder.query([(lat, lon)])[0]["cc"]

def get_worksheets_count(heading_pairs: list, target_headings: list[str] = []) -> int:
    
    if not isinstance(heading_pairs, list):
        return None

    for i, heading in enumerate(heading_pairs):
        if heading in target_headings and i + 1 < len(heading_pairs):
            return heading_pairs[i + 1]

def get_earliest_year(row: pd.Series) -> int:
    timestamp = row.min()
    if pd.isna(timestamp):
        return dt.datetime.fromtimestamp(0.0).year
    return dt.datetime.fromtimestamp(timestamp).year

###############################
#### DF PIPELINE FUNCTIONS ####
###############################

### SRC ROOT

def prepare_dirs():
    return Pipeline(
        [
            Compute(ElementProcessor(get_normalized_path), NameFilter(Cols.SRC_ROOT)),
            Compute(ElementProcessor(is_not_dir), NameFilter(Cols.SRC_ROOT), dest_col=Cols.ROOT_INVALID),
            *flag_dup(Cols.SRC_ROOT, func=duplicated, keep="first"),
            FilterRows(And([Condition(Cols.ROOT_INVALID, "eq", False), Condition(dup_col(Cols.SRC_ROOT), "eq", False)])),
        ]
    )

def add_depth_metrics():
    return Pipeline(
        [
            Compute(ElementProcessor(depth_from_drive), NameFilter(Cols.SRC_ROOT), dest_col=Cols.ROOT_DEPTH),
            Compute(ElementProcessor(tree_depth), NameFilter(Cols.SRC_ROOT), dest_col=Cols.ROOT_TREE_DEPTH),
        ]
    )

### FILES

def flag_dup(col: str, func=duplicated, keep: Literal[False, "first", "last"] = False, labels: dict = None, where: Predicate = None):

    dup = Compute(
        processor=ColProcessor(func, keep=keep),
        col_filter=NameFilter(col),
        dest_col=dup_col(col),
        where=AllRows() if where is None else where
    )

    if labels:
        dup_label = Compute(
            processor=ElementProcessor(label_bool, labels=labels),
            col_filter=NameFilter(dup_col(col)),
            dest_col=dup_label_col(col),
            where=AllRows() if where is None else where
        )

        return [dup, dup_label]

    return [dup]

def assemble_file_path(prefix: Literal["", "Dest"]):

    file_dir_path = dest_col(Cols.FILE_DIR_PATH) if prefix else Cols.FILE_DIR_PATH
    file_path = dest_col(Cols.FILE_PATH) if prefix else Cols.FILE_PATH

    if prefix:
        return Pipeline(
            [
                *flag_dup(Cols.FILE_NAME, func=duplicated_ci, keep="first"),
                Compute(RowProcessor(build_file_path), NameFilter([file_dir_path, Cols.FILE_NAME, dup_col(Cols.FILE_NAME), Cols.INODE]), dest_col=file_path),
            ]
        )
    return Pipeline(
        [
            Compute(RowProcessor(build_file_path), NameFilter([file_dir_path, Cols.FILE_NAME]), dest_col=file_path)
        ]
    )

def add_stat(prefix: Literal["", "Dest"], metrics: list[str]):

    stat = dest_col(Cols.FILE_STAT) if prefix else Cols.FILE_STAT
    file_path = dest_col(Cols.FILE_PATH) if prefix else Cols.FILE_PATH

    size = dest_col(Cols.SIZE) if prefix else Cols.SIZE
    mtime = dest_col(Cols.MODIFIED_AT) if prefix else Cols.MODIFIED_AT
    dev = dest_col(Cols.INODE_DEV) if prefix else Cols.INODE_DEV
    ino = dest_col(Cols.INODE) if prefix else Cols.INODE
    id = dest_col(Cols.FILE_ID) if prefix else Cols.FILE_ID

    stat_metrics = {
        "size": Compute(ElementProcessor(get_size), NameFilter(stat), dest_col=size),
        "mtime": Compute(ElementProcessor(get_mtime), NameFilter(stat), dest_col=mtime),
        "dev": Compute(ElementProcessor(get_dev), NameFilter(stat), dest_col=dev),
        "ino": Compute(ElementProcessor(get_ino), NameFilter(stat), dest_col=ino),
        "id": Compute(ElementProcessor(get_id), NameFilter(stat), dest_col=id)
    }

    return Pipeline(
        [
            Compute(ElementProcessor(safe_stat), NameFilter(file_path), dest_col=stat),
            *[stat_metrics[m] for m in metrics]
        ]
    )

def tag_columns(ctx: Context, *, name_tags: dict = None, keyword_tags: dict = None):
    name_tags = name_tags or {}
    keyword_tags = keyword_tags or {}
    steps = [
        *[AssignTags(KeywordFilter(keywords), tag) for tag, keywords in keyword_tags.items()],
        *[AssignTags(NameFilter(names), tag) for tag, names in name_tags.items()]
        ]
    return Pipeline(steps, context=ctx)

def select_columns(ctx: Context, *, names: list[str] = None, keywords: list[str] = None, tags: list[str] = None):
    names = names or []
    keywords = keywords or []
    tags = tags or []
    return Pipeline(
        [
            FilterCols(
                CombinedFilter(
                    [
                        NameFilter(names),
                        KeywordFilter(keywords),
                        TagFilter(tags)
                    ]
                )
            ),
        ],
        context=ctx
    )

def consolidate_file_ext(ctx: Context):
    return Pipeline(
        [
            Compute(RowProcessor(resolve_ext), NameFilter([Cols.FILE_TYPE_EXT, Cols.FILE_NAME]), Cols.CONSOLIDATED_EXT),
        ],
        context=ctx
    )

def exclude_rows(ctx: Context, *, col: str, values: list):
    conditions = [Condition(col, 'ne', value) for value in values]

    return Pipeline(
        [
            FilterRows(And(conditions))
        ],
        context=ctx
    )

def include_rows(ctx: Context, *, col: str, values: list):
    conditions = [Condition(col, 'eq', value) for value in values]

    return Pipeline(
        [
            FilterRows(And(conditions))
        ],
        context=ctx
    )

def assemble_dest_dir(ctx: Context, dest_root: str, dest_structure: list[str]):
    components_calc = {
        dup_label_col(Cols.FILE_HASH): [
            *flag_dup(Cols.SIZE, func=duplicated, keep=False, where=Condition(Cols.SIZE, "notna")),
            Compute(
                processor=ElementProcessor(calc_full_hash),
                col_filter=NameFilter(Cols.FILE_PATH),
                dest_col=Cols.FILE_HASH,
                where=Condition(dup_col(Cols.SIZE), "eq", True)
            ),
            *flag_dup(Cols.FILE_HASH, func=duplicated, keep="first", labels={True:"dup", False:""}, where=Condition(dup_col(Cols.SIZE), "eq", True)),
        ],
        Cols.FILE_CATEGORY: [
             Compute(
                processor=ColProcessor(pd.DataFrame.fillna, value="Other"),
                col_filter=NameFilter(Cols.FILE_CATEGORY)
            ),
        ],
        Cols.EARLIEST_YEAR: [
            Compute(
                processor=ElementProcessor(ctx.parser.parse),
                col_filter=TagFilter([Tags.CREATE_DT, Tags.ACCESS_DT, Tags.MODIFY_DT])
            ), 
            Compute(
                processor=RowProcessor(get_earliest_year),
                col_filter=TagFilter([Tags.CREATE_DT, Tags.ACCESS_DT, Tags.MODIFY_DT]),
                dest_col=Cols.EARLIEST_YEAR
            )
        ],
        Cols.IMAGE_COUNTRY: [
            Compute(
                processor=RowProcessor(get_country, geocoder=ctx.geocoder),
                col_filter=NameFilter([Cols.EXIF_GPS_LATITUDE, Cols.EXIF_GPS_LONGITUDE]),
                dest_col=Cols.IMAGE_COUNTRY,
                where=Condition(Cols.FILE_CATEGORY, "eq", "Image")
            )
        ],
        Cols.WORKSHEETS_COUNT: [
            Compute(
                processor=ElementProcessor(get_worksheets_count, target_headings=["Worksheets", "Листы"]),
                col_filter=NameFilter(Cols.XML_HEADING_PAIRS), 
                dest_col=Cols.WORKSHEETS_COUNT,
                where=Condition(Cols.FILE_CATEGORY, "eq", "Data-Excel")
            )
        ],
    }

    steps = []
    for layer in dest_structure:
       steps.extend(components_calc.get(layer, []))

    return Pipeline(
        [
            *steps,
            Compute(RowProcessor(build_dir_path, root=dest_root), NameFilter(dest_structure), dest_col=dest_col(Cols.FILE_DIR_PATH))
        ],
        context=ctx
    )