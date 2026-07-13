from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import pandas as pd
from typing import Callable

@dataclass
class Processor(ABC):
    func: Callable
    kwargs: dict = field(default_factory=dict)
    
    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
    
@dataclass
class ElementProcessor(Processor):
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.map(self.func, **self.kwargs)

@dataclass
class RowProcessor(Processor):
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        # Ensure output contract
        result = df.apply(self.func, axis=1, **self.kwargs)
        
        if isinstance(result, pd.Series):
            return result.to_frame()

        return result

@dataclass
class ColProcessor(Processor):
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.func(df, **self.kwargs)