from abc import ABC, abstractmethod
from dataclasses import dataclass
import operator
import pandas as pd
from typing import Literal

@dataclass
class Predicate(ABC):
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

@dataclass
class Condition(Predicate):
    col: str
    comparator: Literal["eq", "ne", "lt", "le", "gt", "ge"]
    val: object

    def apply(self, df: pd.DataFrame) -> pd.Series:
        return getattr(operator, self.comparator)(df[self.col], self.val)

@dataclass
class Or(Predicate):
    conditions: list[Predicate]

    def apply(self, df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(False, index=df.index) # identity for OR
        for cond in self.conditions:
            mask |= cond.apply(df)
        return mask

@dataclass
class And(Predicate):
    conditions: list[Predicate]

    def apply(self, df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(True, index=df.index)
        for cond in self.conditions:
            mask &= cond.apply(df)
        return mask