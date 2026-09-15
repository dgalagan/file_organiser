from cli.components import Errors
from dataclasses import dataclass
from dataframe.write import JSONWriter
from dataframe.load import JSONLoader
import pandas as pd
import os

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
            raise ValueError(Errors.ELEMENTS["empty_input"].build(subject="cache"))

    def load(self) -> None:
        self.data = self.loader.load(self.path)

    def clear(self) -> None:
        self.data = pd.DataFrame()

    def add(self, new_entries: pd.DataFrame) -> None:
        self._require_loaded()
        overlap = new_entries.index.intersection(self.data.index)
        if not overlap.empty:
            raise ValueError(f"New entries overlap with existing")
        self.data = pd.concat([self.data, new_entries])

    def update(self, changed_entries: pd.DataFrame) -> None:
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
        return self.writer.save(self.data, self.path, dropna=dropna)