from dataclasses import dataclass, field
from dataframe.context import Context
from dataframe.write import JSONWriter
from dataframe.load import JSONLoader
from dataframe.predicate import Predicate, Condition, And, Or, AllRows
from exiftool import ExifTool
import json
import os
import pandas as pd
from typing import Iterator

def get_batches(files: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        return [files]
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

@dataclass
class Cache:
    path: str
    loader: JSONLoader
    writer: JSONWriter
    data: pd.DataFrame = None

    def __post_init__(self):
        if not os.path.exists(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.writer.save(pd.DataFrame(), self.path)

    def _require_loaded(self) -> None:
        if self.data is None:
            raise ValueError("Cache not loaded")

    def _require_data(self) -> None:
        self._require_loaded()
        if self.data.empty:
            raise ValueError("Cache is empty")

    def load(self) -> pd.DataFrame:
        self.data = self.loader.load(self.path)

    def clear(self) -> pd.DataFrame:
        self.data = pd.DataFrame()

    def add(self, new_entries: pd.DataFrame) -> None:
        self._require_loaded()
        overlap = new_entries.index.intersection(self.data.index)
        if not overlap.empty:
            raise ValueError(f"New entries overlap with existing")
        self.data = pd.concat([self.data, new_entries])

    def update(self, changed_entries: pd.DataFrame):
        self._require_data()
        self.data.loc[changed_entries.index, changed_entries.columns] = changed_entries

    def clone(self, src_to_dest: dict) -> None:
        self._require_data()
        cloned = self.data.loc[list(src_to_dest.keys())].rename(index=src_to_dest)
        self.add(cloned)

    def delete(self, entry_ids: list) -> None:
        self._require_data()
        self.data = self.data.drop(index=entry_ids, errors="ignore")

    def save(self, dropna: bool = False) -> None:
        self._require_loaded()
        self.writer.save(self.data, self.path, dropna=dropna)

@dataclass
class Reference:
    path: str
    loader: JSONLoader

    def load(self) -> pd.DataFrame:
        return self.loader.load(self.path)

@dataclass
class Exif:
    path: str
    batch_size: int
    args: list[str] = field(default_factory=list)

    def extract(self, files: list[str]) -> Iterator[dict]:
        with ExifTool(encoding="utf-8", executable=self.path) as et:
            for batch in get_batches(files, self.batch_size):
                raw_output = et.execute(*self.args, *batch)
                yield from json.loads(raw_output)

@dataclass
class Config:
    register: Cache
    metadata: Cache
    ref: Reference
    exif: Exif
    context: Context
    # filter: Predicate