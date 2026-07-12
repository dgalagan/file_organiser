from abc import ABC, abstractmethod
import operator
import pandas as pd
from utils.json import load_json
from typing import Callable, Iterator, Literal
from dataclasses import dataclass, field
from core.transformation import DateParser, get_worksheets_count, get_min_year, get_country
import warnings

# all classes should have access to src df (not changed) at any point in time
# sequence of actions might be arbitrary (filter, transform, compute or compute, filter)
# there should be an accumulator df that will consolidate all transformed, computed cols + cols that was used for compute
# filter should be responsible for the boolean mask
# it is necessary for transform or compute to use mask at any step
# so mask might be used temporaryly for one compute or one transform or persistently, so accumulator might be re written with mask parameters
# accumulator df should share the shape of src df, but it should not conflict with mask existence. use case: compute, transform applied to df with mask 

def match_keywords(items: list[str], keywords: list[str]) -> Iterator[tuple[str, str]]:
    for kw in keywords:
        lk = kw.lower()
        for i in items:
            if lk in i.lower():
                yield kw, i

##############  TAG STORE  #############
class TagStore:
    def __init__(self):
        self.tagged_items: dict[str, set] = {}

    @property
    def assigned_tags(self) -> set:
        if not self.tagged_items:
            return set()
        return set().union(*self.tagged_items.values())

    def apply_config(self, tag_cfg: dict[str, dict[str, list[str]]], scope: list[str], default_tag: str = "build"):
        if not tag_cfg:
            return self
        for tag, specs in tag_cfg.items():
            for kw, item in match_keywords(scope, specs.get("keywords", [])):
                self.assign_tags(item, [default_tag, tag, kw])
            for item in specs.get("items", []):
                if item in scope:
                    self.assign_tags(item, [default_tag, tag])
                else: 
                    warnings.warn(f"[Skipped] '{item}' does not belong to match_scope")
        return self

    def assign_tag(self, item: str, tag: str):
        self.tagged_items.setdefault(item, set()).add(tag)
        return self

    def assign_tags(self, item: str, tags: list[str]):
        for tag in tags:
            self.assign_tag(item, tag)
        return self
    
    def rename_tag(self, old_tag: str, new_tag: str):
        if old_tag not in self.assigned_tags:
            raise ValueError(f"Provided tag {old_tag} does not exist")
        for tags in self.tagged_items.values():
            if old_tag in tags:
                tags.remove(old_tag)
                tags.add(new_tag)
        return self

    def find_items(self, tags: str | list[str]) -> list[str]:
        if isinstance(tags, str):
            tags = [tags]
        wanted = set(tags)
        return sorted([item for item, item_tags in self.tagged_items.items() if wanted.intersection(item_tags)])

##############  SHARED CONTEXT  #############
@dataclass
class Context:
    store: TagStore = field(default_factory=TagStore)

##############  PROCESSOR  #############
@dataclass
class Processor(ABC):
    func: Callable
    kwargs: dict = field(default_factory=dict)
    
    @abstractmethod
    def process(self, data: pd.DataFrame):
        raise NotImplementedError
    
@dataclass
class ElementProcessor(Processor):
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.map(self.func, **self.kwargs)

@dataclass
class RowProcessor(Processor):
    def process(self, data: pd.DataFrame) -> pd.Series: # apply to each row reducing result to Series
        return data.apply(self.func, axis=1, **self.kwargs)

@dataclass
class ColProcessor(Processor):
    def process(self, data: pd.DataFrame) -> pd.Series:
        cols = data.columns
        if len(cols) != 1:
            raise ValueError(f"ColProcessor expects one column, got {cols}")
        return self.func(data[cols[0]], **self.kwargs)

##############  COL FILTER    #############
@dataclass
class ColFilter(ABC):
    
    @abstractmethod
    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        raise NotImplementedError
    
    @abstractmethod
    def filter(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        raise NotImplementedError

@dataclass
class NameFilter(ColFilter):
    cols: list[str] | str

    def __post_init__(self):
        if isinstance(self.cols, str):
            self.cols = [self.cols]

    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        return [col for col in self.cols if col in df.columns]

    def filter(self, df: pd.DataFrame, ctx: Context):
        return df[self.select(df, ctx)]

@dataclass
class KeywordFilter(ColFilter):
    keywords: list[str]
    
    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        seen, out = set(), []
        for _, col in match_keywords(df.columns, self.keywords):
            if col not in seen:
                seen.add(col)
                out.append(col)
        return out

    def filter(self, df: pd.DataFrame, ctx: Context):
        return df[self.select(df, ctx)]

@dataclass
class TagFilter(ColFilter):
    tags: list[str]

    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        return [col for col in ctx.store.find_items(self.tags) if col in df.columns]

    def filter(self, df: pd.DataFrame, ctx: Context):
        return df[self.select(df, ctx)]

##############  ROW FILTER   #############
@dataclass
class Predicate(ABC):
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

@dataclass
class Condition(Predicate):
    col: str
    comparator: Literal["eq", "ne", "lt", "le", "gt", "ge"]
    val: object

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return getattr(operator, self.comparator)(df[self.col], self.val)

@dataclass
class Or(Predicate):
    conditions: list[Predicate]

    def apply(self, df: pd.DataFrame):
        mask = pd.Series(False, index=df.index) # identity for OR
        for cond in self.conditions:
            mask |= cond.apply(df)
        return mask

@dataclass
class And(Predicate):
    conditions: list[Predicate]

    def apply(self, df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(True, index=df.index)
        for cond in self.conditions:
            mask &= cond.apply(df)
        return mask
    
##############  PIPELINE   #############

@dataclass
class Step(ABC):
    @abstractmethod
    def run(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        raise NotImplementedError

# Filter
@dataclass
class FilterCols(Step):
    col_filter: ColFilter

    def run(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        return self.col_filter.filter(df, ctx)

@dataclass
class FilterRows(Step):
    predicate: Predicate

    def run(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        mask = self.predicate.apply(df)
        return df[mask]

# Transform
@dataclass
class Transform(Step):
    processor: Processor
    col_filter: ColFilter
    where: Predicate | None = None

    def run(self, df: pd.DataFrame, ctx: Context):
        cols = self.col_filter.select(df, ctx)
        # update values in tag store if available
        if ctx.store is not None:
            for col in cols:
                ctx.store.assign_tag(col, "transformed")
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)
        # compute the result
        result = self.processor.process(df.loc[mask, cols])
        # assign result to df
        if isinstance(result, pd.DataFrame):
            df[cols] = result.reindex(df.index)
        elif isinstance(result, pd.Series):
            df[cols[0]] = result.reindex(df.index)
        else:
            raise ValueError(f"Processor retuned {type(result)}, but pd.Series or pd.DataFrame expected")
        return df

# Compute
@dataclass
class Compute(Step):
    processor: Processor
    col_filter: ColFilter
    output_col: str
    where: Predicate | None = None

    def run(self, df: pd.DataFrame, ctx: Context):
        cols = self.col_filter.select(df, ctx)
        # update values in tag store if available
        if ctx.store is not None:
            ctx.store.assign_tag(self.output_col, "new")
        # init Series[bool] for row filtering
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)
        # compute the result
        result = self.processor.process(df.loc[mask, cols])
        # assign result to df
        if isinstance(result, pd.DataFrame):
            if result.shape[1] != 1:
                raise ValueError(f"Cannot assign a {result.shape[1]}-column result to single output column '{self.output_col}'") 
            df[self.output_col] = result.squeeze(axis=1).reindex(df.index)
        elif isinstance(result, pd.Series):
            df[self.output_col] = result.reindex(df.index)
        else:
            raise ValueError(f"Processor retuned {type(result)}, but pd.Series or pd.DataFrame expected")
        return df
    
############## SCRIPT TEST ##################
exif_db = load_json("db/exif_db.json")

exif_data = {"exif_data": {}}
for path, path_data in exif_db.items():
    path_metadata = path_data["-j-G-all--File:Directory"]["data"]
    exif_data["exif_data"][path] = path_metadata

# Pre-define needed tags
tag_cfg = {
    "created_dt": {
        "keywords": ["createdate", "creationdate", "datetimeoriginal", "datetimedigitized"], #  "createddatetime", "datetimecreated", "encodingtime", "profiledatetime", "retaildate", "ripdate", "releasetime", "originalreleaseyear"
        "items": ["ID3:Year", "EXE:TimeStamp", "XMP:Timestamp", "PNG:ExifDateTime", "Composite:GPSDateTime", "QuickTime:PurchaseDate"]
    },
    "access_dt": {
        "keywords": ["accessdate", "lastplayed", "lastprinted"],
    },
    "modify_dt": {
        "keywords": ["datemodify", "lastsaved", "lastupdated", "moddate", "modifydate", "metadatadate", "sourcemodified"],
    },
    "required": {
        "items": ["File:FileName", "File:FileSize", "File:FileTypeExtension", "XML:HeadingPairs", "EXIF:GPSLatitude", "EXIF:GPSLongitude", "EXIF:Model"]
    }
}

date_parser = DateParser()

process = {
    "exif_data": {
        "tag_cfg": tag_cfg,
        "steps": [
            FilterCols(TagFilter("build")),
            # FilterRows(Condition("File:FileTypeExtension", "eq", "XLS")),
            # FilterRows(And([Condition("File:FileTypeExtension", "eq", "XLS"), Condition("File:FileSize", "ge", 1000000)])),
            Transform(ElementProcessor(date_parser.parse), TagFilter(["created_dt", "modify_dt"])),
            Compute(RowProcessor(get_min_year), TagFilter(["created_dt", "modify_dt"]), "Year"),
            Compute(ElementProcessor(get_worksheets_count), NameFilter("XML:HeadingPairs"), "CountWorksheets"),
            Compute(RowProcessor(get_country, {"lat_col": "EXIF:GPSLatitude", "lon_col": "EXIF:GPSLongitude"}), NameFilter(["EXIF:GPSLatitude", "EXIF:GPSLongitude"]), "Country")
        ],
    }
}

# Execute pipeline
for source, spec in process.items():
    df = pd.DataFrame.from_dict(exif_data.get(source, {}), orient="index")
    if df.empty:
        warnings.warn(f"[{source}] has no data - skipping")
        continue
    ctx = Context(store=TagStore().apply_config(spec.get("tag_cfg", {}), df.columns))
    for step in spec.get("steps"):
        df = step.run(df, ctx)
    df.to_csv("output.csv")