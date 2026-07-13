from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import pandas as pd
from dataframe.context import Context
from dataframe.col_filter import ColFilter
from dataframe.processor import Processor
from dataframe.predicate import Predicate

@dataclass
class Step(ABC):
    @abstractmethod
    def run(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        raise NotImplementedError

# Tag Set Up
@dataclass
class AssignTags(Step):
    col_filter: ColFilter
    tags: list[str]

    def run(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        cols = self.col_filter.select(df, ctx)
        for col in cols:
            ctx.store.assign_tags(col, self.tags)
        return df

# Filter
@dataclass
class FilterCols(Step):
    col_filter: ColFilter

    def run(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        return self.col_filter.filter(df, ctx)

# Filter
@dataclass
class FilterRows(Step):
    predicate: Predicate

    def run(self, df: pd.DataFrame, ctx: Context) -> pd.DataFrame:
        mask = self.predicate.apply(df)
        return df[mask]

# Transform
@dataclass
class Transform(Step):
    processor: Processor
    col_filter: ColFilter
    where: Predicate | None = None

    def run(self, df: pd.DataFrame, ctx: Context):
        cols = self.col_filter.select(df, ctx)
        # update values in tag store if available
        if ctx.store is not None:
            for col in cols:
                ctx.store.assign_tag(col, "transformed")
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)

        result = self.processor.process(df.loc[mask, cols])
        df[cols] = result.reindex(df.index)
        return df

# Compute
@dataclass
class Compute(Step):
    processor: Processor
    col_filter: ColFilter
    output_col: str
    where: Predicate | None = None

    def run(self, df: pd.DataFrame, ctx: Context):
        cols = self.col_filter.select(df, ctx)
        # update values in tag store if available
        if ctx.store is not None:
            ctx.store.assign_tag(self.output_col, "new")
        # init Series[bool] for row filtering
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)

        result = self.processor.process(df.loc[mask, cols])
        df[self.output_col] = result.reindex(df.index)
        return df

@dataclass
class Pipeline:
    steps: list[Step]
    context: Context = field(default_factory=Context)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            df = step.run(df, self.context)
        return df