from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
from typing import Callable

@dataclass(init=False)
class Processor(ABC):
    func: Callable
    kwargs: dict

    def __init__(self, func: Callable, **kwargs):
        self.func = func
        self.kwargs = kwargs
    
    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
    
@dataclass(init=False)
class ElementProcessor(Processor):
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.map(self.func, **self.kwargs)

@dataclass(init=False)
class RowProcessor(Processor):
    def process(self, df: pd.DataFrame) -> pd.DataFrame | pd.Series:
        result = df.apply(self.func, axis=1, **self.kwargs)

        # # Ensure output contract
        # if isinstance(result, pd.Series):
        #     return result.to_frame()

        return result

@dataclass(init=False)
class ColProcessor(Processor):
    def process(self, df: pd.DataFrame) -> pd.DataFrame | pd.Series:
        result = self.func(df, **self.kwargs)

        # # Ensure output contract
        # if isinstance(result, pd.Series):
        #     return result.to_frame()

        return result