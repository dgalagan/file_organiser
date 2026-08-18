from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
from typing import Iterator
from dataframe.context import Context
import warnings

def match_keywords(items: list[str], keywords: list[str]) -> Iterator[tuple[str, str]]:
    for kw in keywords:
        lk = kw.lower()
        for i in items:
            if lk in i.lower():
                yield kw, i

@dataclass
class ColFilter(ABC):
    
    @abstractmethod
    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        raise NotImplementedError
    
    def filter(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        return df[self.select(df, ctx)]

@dataclass
class NameFilter(ColFilter):
    cols: list[str] | str

    def __post_init__(self):
        if isinstance(self.cols, str):
            self.cols = [self.cols]

    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        selected, missing = [], []

        for col in self.cols:
            if col in df.columns:
                selected.append(col)
            else:
                missing.append(col)

        if missing:
            warnings.warn(f"NameFilter: columns not in DataFrame: {missing}")

        return selected

@dataclass
class KeywordFilter(ColFilter):
    keywords: list[str] | str

    def __post_init__(self):
        if isinstance(self.keywords, str):
            self.keywords = [self.keywords]
    
    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        seen, out = set(), []
        for _, col in match_keywords(df.columns, self.keywords):
            if col not in seen:
                seen.add(col)
                out.append(col)
        return out

@dataclass
class TagFilter(ColFilter):
    tags: list[str] | str

    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        return [col for col in ctx.store.find_items(self.tags) if col in df.columns]

@dataclass
class CombinedFilter(ColFilter):
    filters: list[ColFilter]

    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        selected = []
        for filter in self.filters:
            for col in filter.select(df, ctx):
                if col not in selected:
                    selected.append(col)

        # return all if nothing selected

        return selected

@dataclass
class AllCols(ColFilter):

    def select(self, df: pd.DataFrame, ctx: Context) -> list[str]:
        return list(df.columns)