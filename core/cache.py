from core.col_names import ColNames
from dataclasses import dataclass
from dataframe.write import JSONWriter
from dataframe.load import JSONLoader
import os
import pandas as pd
from utils.checks import ensure_not_empty


@dataclass
class Cache:
    path: str
    loader: JSONLoader
    writer: JSONWriter
    schema: dict[ColNames, str] = None
    data: pd.DataFrame = None

    def __post_init__(self):
        if not os.path.exists(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.data = pd.DataFrame(columns=self.schema.keys()).astype(self.schema) if self.schema else pd.DataFrame()
            self.save()
        else:
            self.data = self.loader.load(self.path)

    def _require_loaded(self) -> None:
        if self.data is None:
            raise ValueError("Cache not loaded")

    def _require_data(self) -> None:
        self._require_loaded()
        ensure_not_empty(self.data)

    def _apply_schema(self):
        if self.schema:
            self.data = self.data.astype(self.schema)

    def clear(self) -> None:
         self.data = pd.DataFrame(columns=list(self.schema.keys())).astype(self.schema) if self.schema else pd.DataFrame()

    def add(self, new_entries: pd.DataFrame) -> None:
        self._require_loaded()
        overlap = new_entries.index.intersection(self.data.index)
        if not overlap.empty:
            raise ValueError(f"New entries overlap with existing")
        self.data = pd.concat([self.data, new_entries])
        self._apply_schema()

    def update(self, changed_entries: pd.DataFrame) -> None:
        self._require_data()
        self.data.loc[changed_entries.index, changed_entries.columns] = changed_entries
        self._apply_schema()

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