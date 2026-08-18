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
    tags: list[str] | str

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
# @dataclass
# class Transform(Step):
#     processor: Processor
#     col_filter: ColFilter
#     where: Predicate | None = None

#     def run(self, df: pd.DataFrame, ctx: Context):
#         cols = self.col_filter.select(df, ctx)
#         # update values in tag store if available
#         if ctx.store is not None:
#             for col in cols:
#                 ctx.store.assign_tag(col, "transformed")
#         # init Series[bool] for row filtering
#         mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)
#         # execute calculation
#         result = self.processor.process(df.loc[mask, cols])
#         df[cols] = result.reindex(df.index, fill_value=None)
#         return df

# Compute
@dataclass
class Compute(Step):
    processor: Processor
    col_filter: ColFilter
    dest_col: str | None = None
    where: Predicate | None = None

    def run(self, df: pd.DataFrame, ctx: Context):
        cols = self.col_filter.select(df, ctx)
        # update values in tag store if available
        if ctx.store is not None:
            ctx.store.assign_tags(self.dest_col, "new")
        # init Series[bool] for row filtering
        mask = self.where.apply(df) if self.where else pd.Series(True, index=df.index)
        # execute calculation
        result = self.processor.process(df.loc[mask, cols])
        # print(f"Processor{type(self.processor).__name__} Incoming{type(df.loc[mask, cols])}, Outcoming{type(result)}")
        # assign results
        if self.dest_col:
            if self.dest_col not in df.columns:
                df[self.dest_col] = None
            df.loc[mask, self.dest_col] = result.squeeze() # dtype misalignment issue
            # df[self.dest_col] = result.reindex(df.index, fill_value=None)
        else:
            df[cols] = None
            df.loc[mask, cols] = result
            # df[cols] = result.reindex(df.index, fill_value=None)
        return df

@dataclass
class Pipeline:
    steps: list[Step]
    context: Context = field(default_factory=Context)

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        for step in self.steps:
            df = step.run(df, self.context)
        return df