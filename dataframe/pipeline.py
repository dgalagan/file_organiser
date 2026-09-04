from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import pandas as pd
from dataframe.col_filter import ColumnFilter, AllCols
from dataframe.processor import Processor
from dataframe.predicate import Predicate
from core.tagstore import TagStore

@dataclass
class Step(ABC):
    @abstractmethod
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

# Filter
@dataclass
class FilterCols(Step):
    col_filter: ColumnFilter

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = self.col_filter.select(df.columns)
        return df[cols]

# Filter
@dataclass
class FilterRows(Step):
    predicate: Predicate

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = self.predicate.apply(df)
        return df[mask]

# Expand
@dataclass
class ExpandDict(Step):
    col: str
    where: Predicate = None

    def run(self, df: pd.DataFrame):
        # init Series[bool] for row filtering
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)
        # execute calculation
        result = pd.json_normalize(df.loc[mask, self.col])
        # assign results
        df[result.columns] = None
        df.loc[mask, result.columns] = result
        df = df.drop(columns=self.col)
        return df

# Label
@dataclass
class Label(Step):
    dest_col: str
    value: str # value type probably should be any
    where: Predicate = None

    def run(self, df: pd.DataFrame):
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)
        df.loc[mask, self.dest_col] = self.value
        return df

# Compute
@dataclass
class Compute(Step):
    processor: Processor
    col_filter: ColumnFilter = field(default_factory=AllCols)
    dest_col: str = None
    where: Predicate = None
    tagstore: TagStore = None

    def run(self, df: pd.DataFrame):
        cols = self.col_filter.select(df.columns)
        # init Series[bool] for row filtering
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)
        # execute calculation
        result = self.processor.process(df.loc[mask, cols])
        # assign results
        if self.dest_col:
            if self.dest_col not in df.columns:
                df[self.dest_col] = None
            df.loc[mask, self.dest_col] = result.squeeze() # dtype misalignment issue
            if self.tagstore:
                self.tagstore.assign_tag([self.dest_col], "new")
            # df[self.dest_col] = result.reindex(df.index, fill_value=None)
        else:
            df[cols] = None
            df.loc[mask, cols] = result
            if self.tagstore:
                self.tagstore.assign_tag(cols, "transformed")
            # df[cols] = result.reindex(df.index, fill_value=None)
        return df

@dataclass
class Pipeline:
    steps: list[Step]

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            df = step.run(df)
        return df