from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
from typing import Iterator

def match_keywords(items: list[str], keywords: list[str]) -> Iterator[tuple[str, str]]:
    for kw in keywords:
        lkw = kw.lower()
        for i in items:
            if lkw in i.lower():
                yield kw, i

@dataclass
class ColumnFilter(ABC):
    @abstractmethod
    def select(self, columns: pd.Index) -> list[str]:
        raise NotImplementedError

@dataclass
class NameFilter(ColumnFilter):
    cols: list[str] | str

    def __post_init__(self):
        if isinstance(self.cols, str):
            self.cols = [self.cols]

    def select(self, columns: pd.Index) -> list[str]:
        return [col for col in self.cols if col in columns]

@dataclass
class KeywordFilter(ColumnFilter):
    keywords: list[str] | str

    def __post_init__(self):
        if isinstance(self.keywords, str):
            self.keywords = [self.keywords]
    
    def select(self, columns: pd.Index) -> list[str]:
        seen, out = set(), []
        for kw, col in match_keywords(columns, self.keywords):
            if col not in seen:
                seen.add(col)
                out.append(col)
        return out

@dataclass
class CombinedFilter(ColumnFilter):
    filters: list[ColumnFilter]

    def select(self, columns: pd.Index) -> list[str]:
        selected = []
        for filter in self.filters:
            for col in filter.select(columns):
                if col not in selected:
                    selected.append(col)

        # return all if nothing selected

        return selected

@dataclass
class AllCols(ColumnFilter):
    def select(self, columns: pd.Index) -> list[str]:
        return list(columns)