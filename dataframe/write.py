from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
import os
import json

@dataclass
class Writer(ABC):
    
    @abstractmethod
    def save(self, df: pd.DataFrame, path: str) -> None:
        raise NotImplementedError

@dataclass
class CSVWriter(Writer):
    encoding: str
    file_extension: str = "csv"

    def save(self, df: pd.DataFrame, path: str) -> None:
        # check extension
        # create dir if not exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return df.to_csv(path, encoding=self.encoding)

@dataclass
class JSONWriter(Writer):
    orient: str
    force_ascii: bool
    indent: int
    drop_na: bool
    file_extension: str = "json"

    def save(self, df: pd.DataFrame, path: str) -> None:
        # check extension
        # create dir if not exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if self.drop_na:
            payload = {str(row_id): row.dropna().to_dict() for row_id, row in df.iterrows()}
            with open(path, mode="w", encoding="utf-8") as f:
                json.dump(payload, f, indent=self.indent, ensure_ascii=self.force_ascii)
        else:
            df.to_json(path, orient=self.orient, indent=self.indent, force_ascii=self.force_ascii)