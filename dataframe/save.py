from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
import os
from typing import ClassVar



@dataclass
class Writer(ABC):
    folder: str
    file_name: str
    
    @abstractmethod
    def save(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

@dataclass
class CSVWriter(Writer):
    DEFAULT_ENCODING: ClassVar[str] = "utf-8-sig"

    folder: str
    file_name: str
    file_extension: str = "csv"
    encoding: str = DEFAULT_ENCODING

    def save(self, df: pd.DataFrame) -> None:
        full_name = '.'.join([self.file_name, self.file_extension])
        path = os.path.join(self.folder, full_name)
        return df.to_csv(path, encoding=self.encoding)