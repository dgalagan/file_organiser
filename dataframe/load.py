from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import pandas as pd

class Loader(ABC):

    @abstractmethod
    def load(self) -> pd.DataFrame:
        raise NotImplementedError

@dataclass
class JSONLoader(Loader):
    orient: str

    def load(self, path: str) -> pd.DataFrame:
        return pd.read_json(path, orient=self.orient)