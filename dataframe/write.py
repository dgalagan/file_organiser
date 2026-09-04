from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
import os
import json

@dataclass
class SaveResult:
    path: str
    success: bool
    error: str = None

@dataclass
class Writer(ABC):
    
    @abstractmethod
    def save(self, df: pd.DataFrame, path: str) -> SaveResult:
        raise NotImplementedError

@dataclass
class CSVWriter(Writer):
    encoding: str = "utf-8-sig"
    file_extension: str = "csv"

    def save(self, df: pd.DataFrame, path: str) -> SaveResult:
        # check extension
        # create dir if not exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try: 
            df.to_csv(path, encoding=self.encoding)
            return SaveResult(path, True)
        except Exception as e:
            return SaveResult(path, False, type(e).__name__)

@dataclass
class JSONWriter(Writer):
    orient: str
    force_ascii: bool
    indent: int = 4
    file_extension: str = "json"

    def save(self, df: pd.DataFrame, path: str, dropna: bool = False) -> SaveResult:
        # check extension
        # create dir if not exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            if dropna:
                payload = {str(row_id): row.dropna().to_dict() for row_id, row in df.iterrows()}
                with open(path, mode="w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=self.indent, ensure_ascii=self.force_ascii)
                return SaveResult(path, True)
            else:
                df.to_json(path, orient=self.orient, indent=self.indent, force_ascii=self.force_ascii)
                return SaveResult(path, True)
        except Exception as e:
            return SaveResult(path, False, type(e).__name__)